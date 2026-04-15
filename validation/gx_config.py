import great_expectations as gx
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_gx_expectationSuite(context, asset: str, table: str):

    logger.info("=" * 60)
    logger.info("Configurando Great Expectations - Expectation Suite")

    db_url = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'glow')}:{os.getenv('POSTGRES_PASSWORD', 'glow1234')}"
        f"@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'glow_db')}"
    )
    logger.info("Expectation Suite - Contexto criado")

    # 2. Configurar Data Source SQL
    
    data_source_name = "glow_postgres_source"
    try:
        data_source = context.data_sources.get(data_source_name)
        logger.info(f"Expectation Suite - Data Source recuperado - {data_source}")
    except Exception as e:
        logger.warning(f"Data Source inexistente - {e}")
        data_source = context.data_sources.add_postgres(
            name=data_source_name,
            connection_string=db_url
        )
        logger.info(f"Expectation Suite - Data Source criado - {data_source}")

    # 3. Definir o Data Asset

    asset_name = asset
    try:
        data_asset = data_source.get_asset(asset_name)
        logger.info(f"Expectation Suite - Data Asset recuperado - {data_asset}")

    except Exception as e:
        logger.warning(f"Data Asset inexistente - {e}")
        data_asset = data_source.add_table_asset(
            name=asset_name,
            table_name=table,
            schema_name="raw"
        )
        logger.info(f"Expectation Suite - Data Asset criado - {data_asset}")

    try:
        batch_definition = data_asset.get_batch_definition("batch_full_table")
        logger.info(f"Expectation Suite - Batch definido - {batch_definition}")
    except:
        batch_definition = data_asset.add_batch_definition_whole_table(name="batch_full_table")
        logger.info(f"Expectation Suite - Batch recuperado - {batch_definition}")

    # 4. Criar a Suite de expectativas
    suite_name = f"{asset}_quality_suite"
    expectations_suite = gx.ExpectationSuite(suite_name)
    try:
        expectations_suite = context.suites.add(expectations_suite)
        logger.info(f"Expectation Suite criada - {expectations_suite}")
    except:
        expectations_suite = context.suites.get(suite_name)
        logger.info(f"Expectation Suite recuperada - {expectations_suite}")

    return batch_definition, expectations_suite


if __name__ == "__main__":
    create_gx_expectationSuite()
