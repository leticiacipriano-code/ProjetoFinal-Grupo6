#!/usr/bin/env python
"""
=============================================================
Diagnóstico - GX Docs (Great Expectations)
=============================================================
Verifica se os relatórios do GX estão sendo gerados corretamente
"""

import os
import sys
from pathlib import Path

print("=" * 80)
print("DIAGNÓSTICO - GX Docs (Great Expectations)")
print("=" * 80)

# ─────────────────────────────────────────────
# Verificar caminhos
# ─────────────────────────────────────────────
print("\n📋 Verificando caminhos:")
print("─" * 80)

# Caminho esperado dentro do container
gx_docs_path_container = Path("/app/gx_docs")
print(f"Caminho no container: {gx_docs_path_container}")
print(f"  Existe: {'✓' if gx_docs_path_container.exists() else '✗'}")

if gx_docs_path_container.exists():
    items = list(gx_docs_path_container.iterdir())
    print(f"  Arquivos/pastas: {len(items)}")
    if items:
        for item in items[:5]:
            print(f"    • {item.name}")
        if len(items) > 5:
            print(f"    ... e mais {len(items) - 5}")

# Caminho local (seu PC)
gx_docs_path_local = Path("./gx_docs")
print(f"\nCaminho local: {gx_docs_path_local}")
print(f"  Existe: {'✓' if gx_docs_path_local.exists() else '✗'}")

if gx_docs_path_local.exists():
    items = list(gx_docs_path_local.rglob("*"))
    print(f"  Arquivos/pastas (recursivo): {len(items)}")
    if items:
        for item in items[:5]:
            print(f"    • {item.relative_to(gx_docs_path_local)}")

# ─────────────────────────────────────────────
# Verificar arquivo importante
# ─────────────────────────────────────────────
print("\n📊 Arquivos importantes do GX:")
print("─" * 80)

important_files = [
    "index.html",
    "gx_00__checkpoint_store__glow_checkpoint.html",
    "expectations/",
]

for file in important_files:
    path = gx_docs_path_container / file
    exists = path.exists()
    status = "✓" if exists else "✗"
    print(f"  {status} {file}")

# ─────────────────────────────────────────────
# Informações do GX
# ─────────────────────────────────────────────
print("\n🔧 Informações do GX:")
print("─" * 80)

try:
    import great_expectations as gx
    print(f"  GX versão: {gx.__version__}")
    print(f"  ✓ Great Expectations instalado")
except ImportError:
    print(f"  ✗ Great Expectations NÃO instalado")

# ─────────────────────────────────────────────
# Acessar via Docker
# ─────────────────────────────────────────────
print("\n🐳 Como Verificar via Docker:")
print("─" * 80)
print("  # Ver volume GX")
print("  docker volume ls | grep gx_docs")
print("  ")
print("  # Ver conteúdo do volume")
print("  docker run --rm -v gx_docs:/data alpine ls -la /data")
print("  ")
print("  # Verificar container nginx")
print("  docker ps | grep static_gx")
print("  ")
print("  # Ver logs do nginx")
print("  docker logs static_gx")

# ─────────────────────────────────────────────
# URLs de Acesso
# ─────────────────────────────────────────────
print("\n🌐 URLs para Acessar:")
print("─" * 80)
print("  GX Docs:    http://localhost:8080")
print("  Metabase:   http://localhost:3000")
print("  dbt Docs:   http://localhost:8181")

print("\n" + "=" * 80)
print("✨ Diagnóstico concluído!")
print("=" * 80 + "\n")
