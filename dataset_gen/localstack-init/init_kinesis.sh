#!/bin/sh

echo "Inicializando recursos en LocalStack..."

# Ejecutamos el comando y evaluamos su código de salida directamente
if awslocal kinesis create-stream --stream-name tfm-data-stream --shard-count 1; then
    echo "✅ Stream 'tfm-data-stream' creado exitosamente."
else
    # Si falla, mostramos el error y salimos con código 1 para que Docker registre el fallo
    echo "❌ ERROR: Falló la creación del stream 'tfm-data-stream' en Kinesis." >&2
    exit 1
fi