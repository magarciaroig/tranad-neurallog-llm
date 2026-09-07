"""
llm_client.py - Cliente LLM Unificado y Agnóstico de Proveedor para AIOps / RCA

Capa de abstracción que delega el transporte y protocolos de API en LiteLLM,
incorporando:
1. Carga y validación segura de credenciales (.env con python-dotenv).
2. Mapeo intuitivo de proveedores locales (Ollama) y comerciales (OpenAI, Anthropic, Gemini).
3. Aislamiento riguroso del razonamiento intermedio (Chain of Thought / <think> / reasoning_content)
   para evitar salidas verbosas de modelos Open Source.
4. Purga automática de preámbulos conversacionales para garantizar informes ejecutivos estructurados.
5. Telemetría unificada (latencia, tokens de entrada/salida, modelo y proveedor).
"""

import os
import re
import time
import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Configurar logging silencioso para LiteLLM y SDK de Google GenAI
import litellm
litellm.suppress_debug_info = True
logging.getLogger("google").setLevel(logging.ERROR)
logging.getLogger("google.genai").setLevel(logging.ERROR)
logging.getLogger("google.genai.models").setLevel(logging.ERROR)
logging.getLogger("google_genai").setLevel(logging.ERROR)
logging.getLogger("google_genai.models").setLevel(logging.ERROR)


@dataclass
class LLMResponse:
    """Encapsula la respuesta estandarizada y limpia del LLM."""
    content: str                          # Informe técnico limpio y estructurado
    thinking: Optional[str] = None        # Razonamiento intermedio (Chain of Thought aislado)
    prompt_tokens: int = 0                # Tokens de entrada (prompt)
    completion_tokens: int = 0            # Tokens generados
    total_tokens: int = 0                 # Tokens totales
    latency_seconds: float = 0.0          # Tiempo de inferencia en segundos
    model: str = ""                       # Identificador del modelo ejecutado
    provider: str = ""                    # Proveedor utilizado
    raw_response: Optional[Dict[str, Any]] = field(default=None, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convierte la respuesta a diccionario serializable."""
        return {
            "content": self.content,
            "thinking": self.thinking,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "latency_seconds": round(self.latency_seconds, 3),
            "model": self.model,
            "provider": self.provider
        }

    def render_markdown(self, show_thinking: bool = True) -> str:
        """
        Renderiza la respuesta en formato Markdown enriquecido.
        Si hay razonamiento intermedio, lo encapsula en un acordeón colapsable HTML
        para mantener la salida ejecutiva limpia pero auditable.
        """
        md_parts = []
        if show_thinking and self.thinking and self.thinking.strip():
            thinking_clean = self.thinking.strip()
            char_count = len(thinking_clean)
            md_parts.append(
                f"<details style='margin-bottom: 15px; border: 1px solid #ddd; border-radius: 6px; padding: 8px 12px; background-color: #fcfcfc;'>\n"
                f"<summary style='cursor: pointer; font-weight: 600; color: #4b5563;'>🧠 Ver Cadena de Razonamiento Causal (Chain of Thought - {char_count:,} caracteres)</summary>\n"
                f"<pre style='white-space: pre-wrap; font-family: monospace; font-size: 0.84em; color: #4b5563; margin-top: 10px; max-height: 350px; overflow-y: auto; background: #f3f4f6; padding: 10px; border-radius: 4px;'>\n"
                f"{thinking_clean}\n"
                f"</pre>\n"
                f"</details>\n"
            )

        md_parts.append(self.content.strip())
        return "\n".join(md_parts)

    def display(self, show_thinking: bool = True):
        """
        Renderiza la respuesta en formato Markdown enriquecido dentro de entornos Jupyter.
        Muestra la cadena de pensamiento en un acordeón HTML colapsable y el informe
        directamente como Markdown formateado con encabezados y negritas.
        """
        from IPython.display import display, HTML, Markdown

        if show_thinking and self.thinking and self.thinking.strip():
            thinking_clean = self.thinking.strip()
            char_count = len(thinking_clean)
            html_details = f"""<details style='margin-bottom: 12px; border: 1px solid #d1d5db; border-radius: 6px; padding: 8px 12px; background-color: #f9fafb;'>
<summary style='cursor: pointer; font-weight: 600; color: #374151;'>🧠 Ver Cadena de Razonamiento Causal (Chain of Thought - {char_count:,} caracteres)</summary>
<pre style='white-space: pre-wrap; font-family: monospace; font-size: 0.85em; color: #4b5563; margin-top: 10px; max-height: 300px; overflow-y: auto; background: #ffffff; padding: 10px; border-radius: 4px; border: 1px solid #e5e7eb;'>
{thinking_clean}
</pre>
</details>"""
            display(HTML(html_details))

        display(Markdown(self.content.strip()))


class UnifiedLLMClient:
    """
    Cliente universal que abstrae cualquier modelo local (Ollama) o comercial
    (OpenAI, Anthropic Claude, Google Gemini, Groq, DeepSeek) a través de LiteLLM,
    asegurando salida limpia, sin preámbulos y con aislamiento de razonamiento CoT.
    """

    SUPPORTED_PROVIDERS = {
        "ollama": "ollama_chat/{model}",
        "openai": "openai/{model}",
        "anthropic": "anthropic/{model}",
        "gemini": "gemini/{model}",
        "groq": "groq/{model}",
        "deepseek": "deepseek/{model}",
        "custom": "{model}"
    }

    REQUIRED_KEYS = {
        "openai": ["OPENAI_API_KEY"],
        "anthropic": ["ANTHROPIC_API_KEY"],
        "gemini": ["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        "groq": ["GROQ_API_KEY"],
        "deepseek": ["DEEPSEEK_API_KEY"]
    }

    def __init__(
        self,
        provider: str = "ollama",
        model: str = "deepseek-r1:14b",
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        timeout: int = 300,
        extra_kwargs: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa el cliente unificado.

        Args:
            provider: Identificador del proveedor ('ollama', 'openai', 'anthropic', 'gemini', 'groq', 'deepseek', 'custom').
            model: Nombre del modelo (ej. 'deepseek-r1:14b', 'gpt-4o', 'claude-3-5-sonnet-20241022', 'gemini-2.5-flash').
            api_base: URL base alternativa (por defecto 'http://localhost:11434' para ollama).
            api_key: Clave explícita (opcional, se recomienda configurar mediante archivo .env).
            temperature: Temperatura de muestreo (0.0 a 1.0).
            max_tokens: Límite de tokens en la respuesta.
            timeout: Tiempo máximo de espera en segundos.
            extra_kwargs: Parámetros adicionales a pasar a litellm.completion.
        """
        # Cargar variables de entorno desde .env sin sobreescribir las ya existentes
        load_dotenv(override=False)

        self.provider = provider.lower().strip()
        self.model = model.strip()
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.extra_kwargs = extra_kwargs or {}

        # Resolver API Base
        if self.provider == "ollama":
            self.api_base = api_base or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        else:
            self.api_base = api_base

        # Resolver API Key de forma segura
        self.api_key = api_key or self._resolve_api_key(self.provider)

        # Validar configuración requerida
        self._validate_credentials()

        # Construir identificador de modelo para LiteLLM
        self.litellm_model = self._build_litellm_model(self.provider, self.model)

    def _resolve_api_key(self, provider: str) -> Optional[str]:
        """Obtiene la clave de API desde variables de entorno según el proveedor."""
        possible_keys = self.REQUIRED_KEYS.get(provider, [])
        for k in possible_keys:
            val = os.getenv(k)
            if val and val.strip():
                return val.strip()
        return None

    def _validate_credentials(self):
        """Verifica que las credenciales necesarias existan antes de realizar peticiones."""
        required = self.REQUIRED_KEYS.get(self.provider)
        if required and not self.api_key:
            keys_str = " o ".join(required)
            raise ValueError(
                f"❌ Error de Credenciales: El proveedor '{self.provider}' requiere la variable de entorno {keys_str}.\n"
                f"👉 Por favor, crea un archivo .env en la raíz del proyecto basándote en .env.example:\n"
                f"   cp .env.example .env\n"
                f"   Y añade tu clave: {required[0]}=tu_clave_aqui"
            )

    def _build_litellm_model(self, provider: str, model: str) -> str:
        """Formatea el nombre del modelo con el prefijo correcto que espera LiteLLM."""
        if "/" in model or provider == "custom":
            return model

        template = self.SUPPORTED_PROVIDERS.get(provider, "{model}")
        return template.format(model=model)

    def get_safe_config(self) -> Dict[str, Any]:
        """Devuelve un resumen de configuración con tokens enmascarados para logging seguro."""
        masked_key = None
        if self.api_key:
            masked_key = self.api_key[:4] + "..." + self.api_key[-4:] if len(self.api_key) > 8 else "***"

        return {
            "provider": self.provider,
            "model": self.model,
            "litellm_model": self.litellm_model,
            "api_base": self.api_base,
            "api_key_configured": bool(self.api_key),
            "masked_key": masked_key,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "timeout": self.timeout
        }

    def generate(
        self,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        override_temperature: Optional[float] = None,
        override_max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        json_mode: bool = False,
        response_mime_type: Optional[str] = None
    ) -> LLMResponse:
        """
        Ejecuta la inferencia sobre el modelo configurado y devuelve una respuesta limpia.
        
        Despacha la llamada al driver correspondiente:
        - Para Google Gemini: delega en `_generate_gemini()` mediante el SDK oficial `google-genai`.
        - Para el resto de proveedores (Ollama, OpenAI, Anthropic, Groq, DeepSeek): delega en
          `_generate_litellm()` mediante la capa genérica de LiteLLM.

        Args:
            user_prompt: Texto del prompt de usuario con las evidencias del incidente.
            system_prompt: Instrucciones del sistema (rol, formato, restricciones).
            override_temperature: Temperatura opcional para esta llamada.
            override_max_tokens: Límite de tokens opcional para esta llamada.
            temperature: Alias de override_temperature.
            max_tokens: Alias de override_max_tokens.
            json_mode: Si es True, fuerza la emisión de JSON estructurado a nivel de API.
            response_mime_type: MIME type forzado (ej. 'application/json').

        Returns:
            LLMResponse con el informe estructurado y la cadena de pensamiento desacoplada.
        """
        eff_temp = override_temperature if override_temperature is not None else temperature
        eff_tokens = override_max_tokens if override_max_tokens is not None else max_tokens

        if self.provider == "gemini":
            return self._generate_gemini(
                user_prompt=user_prompt,
                system_prompt=system_prompt,
                override_temperature=eff_temp,
                override_max_tokens=eff_tokens,
                json_mode=json_mode,
                response_mime_type=response_mime_type
            )
        
        return self._generate_litellm(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            override_temperature=eff_temp,
            override_max_tokens=eff_tokens,
            json_mode=json_mode
        )

    def _generate_gemini(
        self,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        override_temperature: Optional[float] = None,
        override_max_tokens: Optional[int] = None,
        json_mode: bool = False,
        response_mime_type: Optional[str] = None
    ) -> LLMResponse:
        """
        Driver especializado para Google Gemini utilizando el SDK oficial `google-genai`.

        =============================================================================
        JUSTIFICACIÓN TÉCNICA DEL CÓDIGO AD-HOC (Por qué no delegar en LiteLLM):
        =============================================================================
        1. Pérdida Crítica del Razonamiento (Chain-of-Thought):
           En modelos reflexivos de Gemini (como Gemini 3.5/3.6 Flash), el SDK `google-genai`
           devuelve las reflexiones en `candidate.content.parts[i]` con el flag `thought=True`.
           LiteLLM contabiliza los tokens de reasoning pero descarta el texto reflexivo
           (`message.reasoning_content = None`), impidiendo la auditoría causal en el acordeón
           interactivo del notebook.
        2. Soporte de ThinkingConfig:
           La nueva API unificada de Google GenAI v1 requiere configurar
           `types.ThinkingConfig(include_thoughts=True)`. LiteLLM carece de abstracción
           nativa para estos parámetros avanzados de razonamiento.
        3. Presupuesto Compartido de Tokens (Shared Output Budget):
           En Gemini 3.x, los tokens de pensamiento y respuesta comparten el cupo de salida.
           El SDK oficial expone `thoughts_token_count` y `finish_reason: MAX_TOKENS`, lo que nos
           permite advertir al usuario sobre truncamientos de forma transparente.
        4. Resiliencia y Backoff Dinámico ante Rate Limits (429 / 503):
           Google AI Studio incluye en los errores 429 directivas de espera exactas
           (`Please retry in Xs` o `retryDelay: '57s'`). Este driver parsea dinámicamente
           dicho retardo y espera el tiempo estricto recomendado por Google, evitando agotar
           los reintentos a ciegas como hace LiteLLM.
        =============================================================================
        """
        t_start = time.perf_counter()
        import warnings
        warnings.filterwarnings("ignore", message=".*automatic function calling.*")
        import logging
        logging.getLogger("google").setLevel(logging.ERROR)
        logging.getLogger("google.genai").setLevel(logging.ERROR)
        logging.getLogger("google.genai.models").setLevel(logging.ERROR)
        logging.getLogger("google_genai").setLevel(logging.ERROR)
        logging.getLogger("google_genai.models").setLevel(logging.ERROR)
        from google import genai
        from google.genai import types

        config_kwargs: Dict[str, Any] = {}
        target_mime = response_mime_type or ("application/json" if json_mode else self.extra_kwargs.get("response_mime_type"))
        if target_mime:
            config_kwargs["response_mime_type"] = target_mime

        client = genai.Client(api_key=self.api_key)
        config = types.GenerateContentConfig(
            system_instruction=system_prompt.strip() if system_prompt and system_prompt.strip() else None,
            temperature=override_temperature if override_temperature is not None else self.temperature,
            max_output_tokens=override_max_tokens if override_max_tokens is not None else self.max_tokens,
            thinking_config=types.ThinkingConfig(
                include_thoughts=True
            ),
            **config_kwargs
        )
        model_clean = self.model.replace("gemini/", "")

        last_err = None
        for attempt in range(6):
            try:
                res = client.models.generate_content(
                    model=model_clean,
                    contents=user_prompt.strip(),
                    config=config
                )
                latency = time.perf_counter() - t_start
                
                cand = res.candidates[0] if (res.candidates and len(res.candidates) > 0) else None
                thinking_parts = []
                content_parts = []
                if cand and cand.content and cand.content.parts:
                    for p in cand.content.parts:
                        if getattr(p, "thought", False):
                            thinking_parts.append(p.text or "")
                        else:
                            content_parts.append(p.text or "")
                
                if thinking_parts:
                    thought_from_parts = "\n".join(thinking_parts)
                    raw_text = "".join(content_parts) if content_parts else (res.text or "")
                    cleaned_content, thought_from_tags = self._extract_thinking_from_text(raw_text)
                    final_thought = (thought_from_parts + ("\n" + thought_from_tags if thought_from_tags else "")).strip()
                else:
                    cleaned_content, final_thought = self._extract_thinking_from_text(res.text or "")

                cleaned_content = self._sanitize_content(cleaned_content)

                # Verificar si la respuesta fue truncada por límite de tokens
                finish_reason_str = str(getattr(cand, "finish_reason", ""))
                if "MAX_TOKENS" in finish_reason_str:
                    print(f"\n⚠️ ADVERTENCIA: La respuesta de Gemini ({model_clean}) fue truncada por límite de tokens (FinishReason: {finish_reason_str}). Aumenta max_tokens.")

                usage = res.usage_metadata
                p_tokens = usage.prompt_token_count if usage else 0
                c_tokens = usage.candidates_token_count if usage else 0
                t_tokens = usage.total_token_count if usage else 0
                th_tokens = getattr(usage, "thoughts_token_count", None)

                return LLMResponse(
                    content=cleaned_content,
                    thinking=final_thought if final_thought else None,
                    prompt_tokens=p_tokens,
                    completion_tokens=c_tokens,
                    total_tokens=t_tokens,
                    latency_seconds=latency,
                    model=self.model,
                    provider=self.provider,
                    raw_response={
                        "finish_reason": finish_reason_str,
                        "thoughts_token_count": th_tokens
                    }
                )
            except Exception as e:
                last_err = e
                err_str = str(e)
                if any(k in err_str.lower() for k in ["perday", "per_day", "daily", "freetier"]):
                    raise RuntimeError(
                        f"❌ Cuota diaria de Gemini ({model_clean}) agotada (GenerateRequestsPerDay): {e}\n"
                        f"👉 Si estás usando el Free Tier de Google AI Studio, este modelo tiene un límite diario (ej. 20 RPD).\n"
                        f"👉 Para eliminar el límite, activa facturación Pay-as-you-go en https://aistudio.google.com/app/plan_information"
                    ) from e
                if any(k in err_str for k in ["503", "UNAVAILABLE", "high demand", "RESOURCE_EXHAUSTED", "429"]):
                    retry_match = re.search(r"retry in (\d+(?:\.\d+)?)s", err_str, re.IGNORECASE)
                    if not retry_match:
                        retry_match = re.search(r"retryDelay\x27?:\s*\x27?(\d+)s", err_str)
                    
                    if retry_match:
                        wait_sec = float(retry_match.group(1)) + 2.0
                    else:
                        wait_sec = (attempt + 1) * 5.0

                    print(f"⏳ Gemini ({model_clean}) ocupado/rate-limited. Esperando {wait_sec:.1f}s antes del reintento {attempt+1}/6...")
                    time.sleep(wait_sec)
                else:
                    raise RuntimeError(f"❌ Error al consultar Gemini ({model_clean}) vía google-genai: {e}") from e
        raise RuntimeError(f"❌ Error al consultar Gemini ({model_clean}) tras 6 reintentos: {last_err}") from last_err

    def _generate_litellm(
        self,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        override_temperature: Optional[float] = None,
        override_max_tokens: Optional[int] = None,
        json_mode: bool = False
    ) -> LLMResponse:
        """
        Driver genérico unificado basado en LiteLLM para proveedores estándar
        (Ollama, OpenAI, Anthropic Claude, Groq, DeepSeek).
        """
        messages = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt.strip()})
        messages.append({"role": "user", "content": user_prompt.strip()})

        params: Dict[str, Any] = {
            "model": self.litellm_model,
            "messages": messages,
            "temperature": override_temperature if override_temperature is not None else self.temperature,
            "max_tokens": override_max_tokens if override_max_tokens is not None else self.max_tokens,
            "timeout": self.timeout,
            **self.extra_kwargs
        }

        if json_mode:
            params["response_format"] = {"type": "json_object"}

        if self.api_base:
            params["api_base"] = self.api_base
        if self.api_key:
            params["api_key"] = self.api_key

        t_start = time.perf_counter()
        try:
            raw_res = litellm.completion(**params)
        except Exception as e:
            # Capturar errores comunes de conexión a Ollama o APIs comerciales
            if "ConnectionRefused" in str(e) or "Failed to connect" in str(e):
                raise ConnectionError(
                    f"❌ No se pudo conectar al endpoint de Ollama ({self.api_base}). "
                    f"Asegúrate de que el servicio está activo con 'ollama serve' o 'ollama list'."
                ) from e
            raise RuntimeError(f"❌ Error al consultar el modelo '{self.litellm_model}' vía LiteLLM: {e}") from e

        latency = time.perf_counter() - t_start

        # Parsear respuesta cruda
        choice = raw_res.choices[0]
        raw_message = choice.message
        raw_content = getattr(raw_message, "content", "") or ""

        # 1. Extraer razonamiento intermedio (Thinking / Chain-of-Thought)
        thinking = getattr(raw_message, "reasoning_content", None)

        # Fallback: si el proveedor vertió <think>...</think> directamente dentro de content
        if not thinking and "<think>" in raw_content:
            cleaned_content, think_from_tags = self._extract_thinking_from_text(raw_content)
            thinking = think_from_tags
            raw_content = cleaned_content

        # 2. Sanitizar preámbulos conversacionales para garantizar salida estructurada
        cleaned_content = self._sanitize_content(raw_content)

        # 3. Métricas de uso de tokens
        usage = getattr(raw_res, "usage", None)
        prompt_tokens = usage.prompt_tokens if usage else 0
        completion_tokens = usage.completion_tokens if usage else 0
        total_tokens = usage.total_tokens if usage else (prompt_tokens + completion_tokens)

        return LLMResponse(
            content=cleaned_content,
            thinking=thinking,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            latency_seconds=latency,
            model=self.model,
            provider=self.provider,
            raw_response=raw_res.model_dump() if hasattr(raw_res, "model_dump") else None
        )

    @staticmethod
    def _extract_thinking_from_text(raw_content: str) -> tuple[str, Optional[str]]:
        """
        Extrae bloques <think>...</think> y devuelve (texto_limpio, pensamiento_extraido).
        """
        thinking = None
        if "<think>" in raw_content:
            think_match = re.search(r"<think>(.*?)</think>", raw_content, flags=re.DOTALL)
            if think_match:
                thinking = think_match.group(1).strip()
                raw_content = re.sub(r"<think>.*?</think>", "", raw_content, flags=re.DOTALL)
        return raw_content.strip(), thinking

    @staticmethod
    def _sanitize_content(text: str) -> str:
        """
        Elimina saludos informales, preámbulos conversacionales o muletillas
        para que el informe técnico comience inmediatamente en el encabezado de diagnóstico.
        """
        text = text.strip()
        # Si el texto ya empieza con un encabezado markdown, está limpio
        if text.startswith("#"):
            return text

        # Buscar el primer encabezado Markdown (### 1. Diagnóstico... o # ...)
        header_match = re.search(r"(###?\s+[0-9]?\.?\s*Diagnóstico.*)", text, flags=re.IGNORECASE | re.DOTALL)
        if header_match:
            return header_match.group(1).strip()

        # Fallback para encabezados genéricos
        generic_header = re.search(r"(###?\s+.*)", text, flags=re.DOTALL)
        if generic_header:
            return generic_header.group(1).strip()

        # Purgar saludos típicos en primera línea si no hay encabezados explícitos
        text = re.sub(
            r"^[\s\n]*(¡?(?:Claro|Hola|Por supuesto|Entendido|Aquí tienes|A continuación|Sure|Here is|Hello|Certainly)[^#\n]*\n+)+",
            "",
            text,
            flags=re.IGNORECASE
        )
        return text.strip()
