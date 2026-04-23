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
from sqlalchemy import create_engine, text, inspect
import logging
import os
import sys
import pandas as pd
import time
import socket


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

# Detecta se está rodando dentro de um container ou localmente
IS_DOCKER = os.path.exists("/.dockerenv") or os.getenv("RUNNING_IN_DOCKER") == "true"

# Configuração de host: usa o hostname do container se em Docker
POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "glow_postgres" if IS_DOCKER else "localhost"
)
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "glow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "glow1234")
POSTGRES_DB = os.getenv("POSTGRES_DB", "glow_db")
BASE_DATA_PATH = Path(os.getenv("BASE_DATA_PATH", "./base"))

# Constrói URL de conexão
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

def get_engine(retries=5, delay=2):
    """
    Cria e retorna um engine SQLAlchemy com pool de conexões.
    Implementa retry em caso de falha de conexão (útil em Docker durante inicialização).
    
    Args:
        retries: número de tentativas de conexão
        delay: segundos entre tentativas
    """
    logger.info(f"Conectando ao banco: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(
                DB_URL,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 10}
            )
            
            # Testa a conexão
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"✓ Conexão estabelecida com sucesso (tentativa {attempt}/{retries})")
            return engine
            
        except Exception as e:
            if attempt < retries:
                logger.warning(f"✗ Falha na conexão (tentativa {attempt}/{retries}): {e}")
                logger.info(f"  Aguardando {delay}s antes de tentar novamente...")
                time.sleep(delay)
            else:
                logger.error(f"✗ Falha após {retries} tentativas:")
                logger.error(f"  Host: {POSTGRES_HOST}:{POSTGRES_PORT}")
                logger.error(f"  Erro: {e}")
                raise


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
    """
    Aguarda a criação/disponibilidade da tabela no PostgreSQL.
    Essencial quando tabelas são criadas de forma assíncrona ou
    quando o schema ainda está sendo propagado em ambientes containerizados.
    
    Args:
        engine: SQLAlchemy engine
        table_name: nome da tabela a verificar
        schema: nome do schema (padrão: 'raw')
        timeout: tempo máximo de espera em segundos
    
    Levanta TimeoutError se a tabela não for criada no timeout.
    """
    start_time = time.time()
    
    logger.info(f"Aguardando criação de {schema}.{table_name} (timeout: {timeout}s)...")
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed > timeout:
            logger.error(
                f"✗ Timeout ao aguardar {schema}.{table_name} "
                f"(após {elapsed:.1f}s)"
            )
            raise TimeoutError(
                f"Tabela {schema}.{table_name} não foi criada em {timeout}s"
            )
        
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema=schema)
            
            if table_name in tables:
                logger.info(
                    f"✓ Tabela {schema}.{table_name} está pronta "
                    f"(detectada em {elapsed:.1f}s)"
                )
                return True
        
        except Exception as e:
            logger.debug(f"Verificação de tabela falhou (tentando novamente): {e}")
        
        # Aguarda 2 segundos antes da próxima tentativa
        time.sleep(2)


# ─────────────────────────────────────────────
# 6. Orquestração principal
# ─────────────────────────────────────────────

def run_ingestion(engine=None):
    """
    Executa o pipeline EL completo:
      1. Conecta ao PostgreSQL (com retry automático em Docker)
      2. Garante schema raw e tabela de log
      3. Para cada fonte no catálogo: lê → normaliza → carrega → confirma
      4. Carrega tabela de referência de ingredientes de risco
      5. Valida que os dados estão acessíveis para transformações dbt
      6. Imprime resumo final
    """
    logger.info("=" * 70)
    logger.info("🚀 Glow & Co. — Pipeline EL (Extract & Load) iniciado")
    logger.info(f"📁 Base path: {BASE_DATA_PATH.resolve()}")
    logger.info(f"� Modo de execução: {'🐳 DOCKER' if IS_DOCKER else '🖥️  LOCAL'}")
    logger.info(f"�🐘 PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info("=" * 70)

    start_time = datetime.now()
    summary = {"success": 0, "failed": 0, "skipped": 0}

    # Se a engine não for passada - criar (com retry automático)
    if engine is None:
        try:
            engine = get_engine(retries=5, delay=2)
        except Exception as e:
            logger.error("✗ Falha ao conectar ao PostgreSQL - abortando ingestão")
            sys.exit(1)

    # Garante schemas e tabelas de suporte
    ensure_raw_schema(engine)
    ensure_ingestion_log(engine)
    
    logger.info(f"✓ Schema e tabelas de suporte verificados")

    loaded_tables = []

    # ────────────────────────────────────────────────────────────────────
    # FASE 1: Ingestão das fontes de dados brutos
    # ────────────────────────────────────────────────────────────────────
    logger.info("\n" + "─" * 70)
    logger.info("FASE 1: Ingestão de Fontes de Dados Brutos")
    logger.info("─" * 70)

    for source in SOURCE_CATALOG:
        logger.info(f"\n▶ Processando: {source['file']}")
        logger.info(f"  └─ Tabela destino: raw.{source['table']}")
        logger.info(f"  └─ Descrição: {source['description']}")

        # Lê o arquivo de origem
        df = read_source_file(source)

        if df is None:
            logger.warning("  ✗ Arquivo não encontrado - pulando")
            summary["skipped"] += 1
            log_ingestion(engine, source["table"], source["file"], 0, "SKIPPED", 
                         "Arquivo não encontrado")
            continue

        try:
            # Normaliza e adiciona metadados
            df = normalize_columns(df)
            df = add_metadata(df, source["file"])
            
            # Carrega dados
            rows = load_to_postgres(df, source["table"], engine)
            
            # Aguarda confirmação da tabela (importante em ambientes Docker)
            wait_for_table(engine, source["table"], schema="raw", timeout=30)
            
            # Valida dados carregados
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM raw.{source['table']}")
                )
                actual_count = result.scalar()
                
                if actual_count == rows:
                    logger.info(f"  ✓ Carregadas {rows:,} linhas em raw.{source['table']}")
                    logger.info(f"  ✓ Validação OK ({actual_count:,} linhas confirmadas)")
                else:
                    logger.warning(
                        f"  ⚠ Discrepância: esperado {rows}, "
                        f"encontrado {actual_count}"
                    )
            
            loaded_tables.append(source["table"])
            log_ingestion(engine, source["table"], source["file"], rows, "SUCCESS")
            summary["success"] += 1

        except Exception as exc:
            logger.error(f"  ✗ Erro ao carregar: {exc}")
            log_ingestion(engine, source["table"], source["file"], 0, "FAILED", 
                         str(exc))
            summary["failed"] += 1

    # ────────────────────────────────────────────────────────────────────
    # FASE 2: Carregamento de Tabelas de Referência
    # ────────────────────────────────────────────────────────────────────
    logger.info("\n" + "─" * 70)
    logger.info("FASE 2: Carregamento de Tabelas de Referência")
    logger.info("─" * 70)
    
    logger.info("\n▶ Carregando tabela de referência: ingredientes de risco")
    try:
        df_risk = pd.DataFrame(RISK_INGREDIENTS)
        df_risk = add_metadata(df_risk, "in-code reference")
        rows = load_to_postgres(df_risk, "ref_risk_ingredients", engine)
        
        # Aguarda confirmação
        wait_for_table(engine, "ref_risk_ingredients", schema="raw", timeout=30)
        
        logger.info(f"  ✓ Carregadas {rows:,} linhas em raw.ref_risk_ingredients")
        log_ingestion(engine, "ref_risk_ingredients", "in-code reference", rows, 
                     "SUCCESS")
        summary["success"] += 1
        
    except Exception as exc:
        logger.error(f"  ✗ Erro ao carregar ingredientes de risco: {exc}")
        log_ingestion(engine, "ref_risk_ingredients", "in-code reference", 0, 
                     "FAILED", str(exc))
        summary["failed"] += 1

    # ────────────────────────────────────────────────────────────────────
    # FASE 3: Validação Final
    # ────────────────────────────────────────────────────────────────────
    logger.info("\n" + "─" * 70)
    logger.info("FASE 3: Validação Final")
    logger.info("─" * 70)
    
    logger.info("\nVerificando acessibilidade das tabelas para transformações dbt:")
    try:
        inspector = inspect(engine)
        raw_tables = inspector.get_table_names(schema="raw")
        
        for table in loaded_tables + ["ref_risk_ingredients"]:
            if table in raw_tables:
                with engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT COUNT(*) as cnt FROM raw.{table}")
                    )
                    count = result.scalar()
                    logger.info(f"  ✓ raw.{table}: {count:,} linhas [OK para dbt]")
            else:
                logger.warning(f"  ✗ raw.{table}: não encontrada")
    
    except Exception as e:
        logger.error(f"  ✗ Erro ao validar tabelas: {e}")

    # ────────────────────────────────────────────────────────────────────
    # RESUMO FINAL
    # ────────────────────────────────────────────────────────────────────
    elapsed = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "=" * 70)
    logger.info("📊 RESUMO DA INGESTÃO")
    logger.info("=" * 70)
    logger.info(f"  ✓ Sucesso  : {summary['success']}")
    logger.info(f"  ✗ Falha    : {summary['failed']}")
    logger.info(f"  – Pulados  : {summary['skipped']}")
    logger.info(f"⏱  Tempo total: {elapsed:.1f}s")
    logger.info("=" * 70)
    
    if summary["failed"] > 0:
        logger.error("\n❌ Pipeline finalizado com erros")
        sys.exit(1)
    else:
        logger.info("\n✅ Pipeline EL concluído com sucesso!")
        logger.info(f"📊 Total de tabelas criadas: {summary['success']}")
        logger.info(f"🎯 Dados prontos para transformações dbt em: raw.*")
        
    return loaded_tables


if __name__ == "__main__":
    run_ingestion()
