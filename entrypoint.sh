#!/bin/sh

echo "Resetando volume GX..."

rm -rf /app/gx_docs/*
mkdir -p /app/gx_docs

echo "Iniciando ap..."

exec uv run main.py