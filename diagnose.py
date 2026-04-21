#!/usr/bin/env python
"""
=============================================================
Script de Diagnóstico - Verificar Conectividade
=============================================================
Testa conexão com PostgreSQL e Metabase
"""

from dotenv import load_dotenv
import os
import sys
import time

print("=" * 80)
print("DIAGNÓSTICO - Verificação de Conectividade")
print("=" * 80)

load_dotenv()

# ─────────────────────────────────────────────
# Verificar Variáveis de Ambiente
# ─────────────────────────────────────────────
print("\n📋 Variáveis de Ambiente:")
print("─" * 80)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "glow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "glow1234")
POSTGRES_DB = os.getenv("POSTGRES_DB", "glow_db")

print(f"  POSTGRES_HOST:     {POSTGRES_HOST}")
print(f"  POSTGRES_PORT:     {POSTGRES_PORT}")
print(f"  POSTGRES_USER:     {POSTGRES_USER}")
print(f"  POSTGRES_PASSWORD: {'*' * len(POSTGRES_PASSWORD)}")
print(f"  POSTGRES_DB:       {POSTGRES_DB}")

# ─────────────────────────────────────────────
# Testar PostgreSQL
# ─────────────────────────────────────────────
print("\n🐘 Testando PostgreSQL:")
print("─" * 80)

try:
    import psycopg2
    
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        connect_timeout=5
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    
    print(f"✓ Conexão bem-sucedida!")
    print(f"  PostgreSQL: {version[0][:50]}...")
    
    # Listar schemas
    cursor.execute("SELECT schema_name FROM information_schema.schemata;")
    schemas = cursor.fetchall()
    print(f"  Schemas: {', '.join([s[0] for s in schemas])}")
    
    # Verificar tabelas em schema raw
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' OR table_schema = 'raw'
        ORDER BY table_schema, table_name
    """)
    tables = cursor.fetchall()
    if tables:
        print(f"  Tabelas: {', '.join([t[0] for t in tables[:5]])}")
        if len(tables) > 5:
            print(f"           ... e mais {len(tables) - 5}")
    else:
        print(f"  Tabelas: Nenhuma encontrada")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"✗ Falha ao conectar:")
    print(f"  {type(e).__name__}: {e}")
    sys.exit(1)

# ─────────────────────────────────────────────
# Testar Metabase
# ─────────────────────────────────────────────
print("\n📊 Testando Metabase:")
print("─" * 80)

try:
    import requests
    
    response = requests.get(
        "http://localhost:3000/api/health",
        timeout=5
    )
    
    if response.status_code == 200:
        print(f"✓ Metabase está disponível")
        print(f"  Status: {response.status_code}")
    else:
        print(f"⚠ Metabase retornou: {response.status_code}")
        print(f"  Response: {response.text[:100]}")
        
except Exception as e:
    print(f"⚠ Metabase não respondeu:")
    print(f"  {type(e).__name__}: {e}")
    print(f"  Certifique-se que Metabase está rodando em localhost:3000")

# ─────────────────────────────────────────────
# Resumo
# ─────────────────────────────────────────────
print("\n" + "=" * 80)
print("✓ Diagnóstico concluído!")
print("=" * 80 + "\n")
