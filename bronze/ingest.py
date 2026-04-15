"""
=============================================================
Glow & Co. — Pipeline EL (Extract & Load) — Camada Raw
=============================================================
Responsabilidade:
  Lê os arquivos de origem (CSV / XLS) da pasta `base/`,
  adiciona metadados de rastreabilidade e carrega cada
  dataset no schema `raw` do PostgreSQL.

Execução:
  python bronze/ingest.py

Variáveis de ambiente (via .env):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
  POSTGRES_PASSWORD, POSTGRES_DB, BASE_DATA_PATH
=============================================================
"""

from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text
import logging
import os
import sys
import pandas as pd
import time


# ─────────────────────────────────────────────
# 1. Configuração de Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bronze/ingest.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 2. Carrega variáveis de ambiente
# ─────────────────────────────────────────────
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "glow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "glow1234")
POSTGRES_DB = os.getenv("POSTGRES_DB", "glow_db")
BASE_DATA_PATH = Path(os.getenv("BASE_DATA_PATH", "./base"))

DB_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ─────────────────────────────────────────────
# 3. Catálogo de arquivos de origem
#    Cada entrada mapeia:
#      file        → caminho relativo à BASE_DATA_PATH
#      table       → nome da tabela no schema raw
#      description → contexto de negócio
# ─────────────────────────────────────────────
SOURCE_CATALOG = [
    {
        "file": "cosmetics.csv",
        "table": "cosmetics_products",
        "description": "Produtos cosméticos com ingredientes, preço e ranking por tipo de pele",
        "read_kwargs": {"encoding": "utf-8"},
    },
    {
        "file": "skincare_products_clean.csv",
        "table": "skincare_products",
        "description": "Produtos de skincare com lista de ingredientes limpa e preço em GBP",
        "read_kwargs": {"encoding": "utf-8"},
    },
    {
        "file": "cosmetics_sales_data.csv",
        "table": "sales_data",
        "description": "Dados de vendas de 2022: produto, país, vendedor, valor e caixas",
        "read_kwargs": {"encoding": "utf-8"},
    },
    {
        "file": "COSING_Ingredients-Fragrance Inventory_v2.csv",
        "table": "cosing_ingredients",
        "description": "Inventário COSING de ingredientes e fragrâncias com funções e restrições",
        "read_kwargs": {"encoding": "utf-8", "sep": ",", "on_bad_lines": "skip"},
    },
    {
        "file": "COSING_Annex_II_v2.xls",
        "table": "cosing_annex_ii",
        "description": "COSING Annex II: substâncias proibidas em cosméticos na UE",
        "read_kwargs": {},
        "is_xls": True,
    },
    {
        "file": "COSING_Annex_III_v2.xls",
        "table": "cosing_annex_iii",
        "description": "COSING Annex III: substâncias com restrições de uso",
        "read_kwargs": {},
        "is_xls": True,
    },
    {
        "file": "COSING_Annex_IV_v2.xls",
        "table": "cosing_annex_iv",
        "description": "COSING Annex IV: corantes permitidos em cosméticos",
        "read_kwargs": {},
        "is_xls": True,
    },
    {
        "file": "COSING_Annex_V_v2.xls",
        "table": "cosing_annex_v",
        "description": "COSING Annex V: conservantes permitidos em cosméticos",
        "read_kwargs": {},
        "is_xls": True,
    },
]

# ─────────────────────────────────────────────
# 4. Tabela de referência: ingredientes de risco
#    Criada a partir de conhecimento de domínio
#    (não tem arquivo de origem — gerada in-code)
# ─────────────────────────────────────────────
RISK_INGREDIENTS = [
    {"ingrediente": "paraben",       "categoria": "Conservante",       "risco": "Alto",      "motivo": "Desregulação Endócrina"},
    {"ingrediente": "sulfate",       "categoria": "Surfactante",        "risco": "Médio",     "motivo": "Irritação Cutânea / Ressecamento"},
    {"ingrediente": "fragrance",     "categoria": "Fragrância",         "risco": "Alto",      "motivo": "Alergia não especificada"},
    {"ingrediente": "phthalate",     "categoria": "Fixador",            "risco": "Alto",      "motivo": "Toxicidade Reprodutiva"},
    {"ingrediente": "formaldehyde",  "categoria": "Conservante",        "risco": "Muito Alto","motivo": "Carcinogênico / Alergia Severa"},
    {"ingrediente": "linalool",      "categoria": "Fragrância Natural", "risco": "Baixo",     "motivo": "Sensibilizante em oxidação"},
]


# ─────────────────────────────────────────────
# 5. Funções auxiliares
# ─────────────────────────────────────────────

def get_engine():
    """Cria e retorna um engine SQLAlchemy com pool de conexões."""
    logger.info(f"Conectando ao banco: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    engine = create_engine(DB_URL, pool_pre_ping=True)
    # Testa a conexão antes de retornar
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Conexão estabelecida com sucesso.")
    return engine


def ensure_raw_schema(engine):
    """Garante que o schema `raw` existe no banco."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()
    logger.info("Schema `raw` verificado/criado.")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas para snake_case:
    espaços → underscore, letras minúsculas, remove caracteres especiais.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


def add_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Adiciona colunas de rastreabilidade a cada linha:
      _source_file  → nome do arquivo de origem
      _loaded_at    → timestamp UTC da carga
    """
    df["_source_file"] = source_file
    df["_loaded_at"] = datetime.now(timezone.utc)
    return df


def read_source_file(source: dict) -> pd.DataFrame | None:
    """
    Lê um arquivo de origem (CSV ou XLS) e retorna um DataFrame.
    Retorna None em caso de falha, sem interromper o pipeline.
    """
    file_path = BASE_DATA_PATH / source["file"]

    if not file_path.exists():
        logger.warning(f"Arquivo não encontrado: {file_path} — pulando.")
        return None

    try:
        if source.get("is_xls"):
            df = pd.read_excel(file_path, engine="xlrd", **source["read_kwargs"])
        else:
            df = pd.read_csv(file_path, **source["read_kwargs"])

        logger.info(f"  Lido: {source['file']} → {len(df):,} linhas | {df.shape[1]} colunas")
        return df

    except Exception as exc:
        logger.error(f"  Falha ao ler '{source['file']}': {exc}")
        return None


def load_to_postgres(df: pd.DataFrame, table_name: str, engine) -> int:
    """
    Carrega o DataFrame para o PostgreSQL (schema raw).
    Estratégia: replace (recarga completa a cada execução).
    Retorna o número de linhas carregadas.
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema="raw",
        if_exists="replace",   # substitui a tabela inteira (idempotente)
        index=False,
        chunksize=1000,
        method="multi",
    )
    return len(df)


def log_ingestion(engine, table: str, source_file: str, rows: int, status: str, error: str = None):
    """
    Registra cada ingestão em raw._ingestion_log para rastreabilidade.
    """
    sql = text("""
        INSERT INTO raw._ingestion_log
            (table_name, source_file, rows_loaded, status, error_message, loaded_at)
        VALUES
            (:table_name, :source_file, :rows_loaded, :status, :error_message, :loaded_at)
    """)
    with engine.connect() as conn:
        conn.execute(sql, {
            "table_name": table,
            "source_file": source_file,
            "rows_loaded": rows,
            "status": status,
            "error_message": error,
            "loaded_at": datetime.now(timezone.utc),
        })
        conn.commit()


def ensure_ingestion_log(engine):
    """Cria a tabela de log de ingestão se não existir."""
    ddl = text("""
        CREATE TABLE IF NOT EXISTS raw._ingestion_log (
            id            SERIAL PRIMARY KEY,
            table_name    TEXT NOT NULL,
            source_file   TEXT NOT NULL,
            rows_loaded   INTEGER,
            status        TEXT NOT NULL,
            error_message TEXT,
            loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    with engine.connect() as conn:
        conn.execute(ddl)
        conn.commit()
    logger.info("Tabela raw._ingestion_log verificada/criada.")


def wait_for_table(engine, table_name, schema="raw", timeout=60):
    """Espera a criação da tabela no container do Postgres para que os outros serviços possam encontrá-las lá."""
    start = time.time()

    while True:
        
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"""
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    """))
                if result.fetchone():
                    print(f"Table {schema}.{table_name} is ready!")
                    return
        
        except Exception as e:
            print(f"Waiting for table... ({e})")

        if time.time() - start > timeout:
            raise TimeoutError(f"Timeout waiting for table {schema}.{table_name}")

        time.sleep(2)


# ─────────────────────────────────────────────
# 6. Orquestração principal
# ─────────────────────────────────────────────

def run_ingestion(engine=None):
    """
    Executa o pipeline EL completo:
      1. Conecta ao PostgreSQL
      2. Garante schema raw e tabela de log
      3. Para cada fonte no catálogo: lê → normaliza → carrega
      4. Carrega tabela de referência de ingredientes de risco
      5. Imprime resumo final
    """
    logger.info("=" * 60)
    logger.info("Glow & Co. — Ingestão Raw iniciada")
    logger.info(f"Base path: {BASE_DATA_PATH.resolve()}")
    logger.info("=" * 60)

    start_time = datetime.now()
    summary = {"success": 0, "failed": 0, "skipped": 0}

    # Se a engine não for passada - criar
    if engine is None:
        engine = get_engine()

    ensure_raw_schema(engine)
    ensure_ingestion_log(engine)

    loaded_tables = []

    # ── Itera sobre o catálogo de fontes ──
    for source in SOURCE_CATALOG:
        logger.info(f"\n▶ Processando: {source['file']}")
        logger.info(f"  Tabela destino: raw.{source['table']}")
        logger.info(f"  Descrição: {source['description']}")

        df = read_source_file(source)

        if df is None:
            summary["skipped"] += 1
            log_ingestion(engine, source["table"], source["file"], 0, "SKIPPED")
            continue

        try:
            df = normalize_columns(df)
            df = add_metadata(df, source["file"])
            rows = load_to_postgres(df, source["table"], engine)
            loaded_tables.append(source["table"])

            logger.info(f"  ✓ Carregadas {rows:,} linhas em raw.{source['table']}")
            log_ingestion(engine, source["table"], source["file"], rows, "SUCCESS")
            summary["success"] += 1

        except Exception as exc:
            logger.error(f"  ✗ Erro ao carregar '{source['table']}': {exc}")
            log_ingestion(engine, source["table"], source["file"], 0, "FAILED", str(exc))
            summary["failed"] += 1

    # ── Tabela de referência de ingredientes de risco ──
    logger.info("\n▶ Carregando tabela de referência: ingredientes de risco")
    try:
        df_risk = pd.DataFrame(RISK_INGREDIENTS)
        df_risk = add_metadata(df_risk, "in-code reference")
        rows = load_to_postgres(df_risk, "ref_risk_ingredients", engine)
        logger.info(f"  ✓ Carregadas {rows:,} linhas em raw.ref_risk_ingredients")
        log_ingestion(engine, "ref_risk_ingredients", "in-code reference", rows, "SUCCESS")
        summary["success"] += 1
    except Exception as exc:
        logger.error(f"  ✗ Erro ao carregar ref_risk_ingredients: {exc}")
        summary["failed"] += 1

    # ── Resumo ──
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "=" * 60)
    logger.info("Resumo da ingestão:")
    logger.info(f"  ✓ Sucesso : {summary['success']}")
    logger.info(f"  ✗ Falha   : {summary['failed']}")
    logger.info(f"  – Pulados : {summary['skipped']}")
    logger.info(f"  Tempo total: {elapsed:.1f}s")
    logger.info("=" * 60)

    if summary["failed"] > 0:
        sys.exit(1)
    else:
        return loaded_tables


if __name__ == "__main__":
    run_ingestion()
