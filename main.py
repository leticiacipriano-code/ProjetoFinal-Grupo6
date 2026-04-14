import logging
from bronze.ingest import run_ingestion


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    logger.info("Iniciando Pipeline Glow & Co.")

    # 1. Bronze Layer (EL)
    logger.info("Etapa 1: Ingestão de dados (Bronze)")
    run_ingestion()

    # 2. Quality Layer (GX)

    logger.info("Pipeline finalizado com sucesso !")


if __name__ == "__main__":
    main()
