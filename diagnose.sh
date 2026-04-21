#!/bin/bash
# Script para executar diagnóstico dentro do container Docker (g6)

echo "=================================================="
echo "Executando diagnóstico dentro do container 'g6'"
echo "=================================================="
echo ""

docker-compose exec g6 python diagnose.py
