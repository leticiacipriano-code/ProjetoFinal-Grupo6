from dotenv import load_dotenv
import logging
import sys

# 1. Carregar as variáveis de ambiente
load_dotenv()

from bronze.ingest import get_engine, wait_for_table, run_ingestion
from validation.gx_run import run_gx_validation


# Configuração de Logs para aparecer bonito no terminal da apresentação
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MAIN_PIPELINE")


def main():

    try:
        logger.info("Iniciando Pipeline Glow & Co.")

        # Conexão
        try:
            engine = get_engine()
        except Exception as exc:
            logger.critical(f"Não foi possível conectar ao banco: {exc}")
            sys.exit(1)

        # 1. Bronze Layer (EL)
        logger.info("Etapa 1: Ingestão de dados (Bronze)")
        loaded_tables = run_ingestion(engine)

        # Espera o Container ser populado com as tabelas antes do gx ser chamado.
        for table in loaded_tables:
            wait_for_table(engine, table)

        # Passo 2: Qualidade (Great Expectations)
        logger.info("Etapa 2: Validação de Qualidade (GX)...")
        run_gx_validation()

        logger.info("Pipeline finalizado com sucesso!")
        logger.info("Acesse: Metabase (3000) | GX Docs (8080) | dbt Docs (8181)")

    except Exception as e:
        logger.error(f"Falha crítica no pipeline: {e}")
        exit(1)

if __name__ == "__main__":
    main()
