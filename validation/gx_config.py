import great_expectations as gx
import logging
import os
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_quality_checks():
    
    # 1. Contexto e conexão (usando as variáveis do .env)
    
    context = gx.get_context(mode="file", project_root_dir="/app/gx_docs")

    db_url = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'glow')}:{os.getenv('POSTGRES_PASSWORD', 'glow1234')}"
        f"@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'glow_db')}"
    )

    # 2. Configurar Data Source SQL
    
    data_source_name = "glow_postgres_source"
    if data_source_name not in [data_source.name for data_source in context.data_sources.all()]:
        data_source = context.data_sources.add_postgres(
            name=data_source_name,
            connection_string=db_url
        )
    else:
        data_source = context.data_sources.get(data_source_name)
    
    # 3. Definir o Asset (raw.cosmetics_products)

    asset_name = "raw_cosmetics"
    if asset_name not in [asset.name for asset in data_source.assets]:
        data_asset = data_source.add_table_asset(
            name=asset_name,
            table_name="cosmetics_products",
            schema_name="raw"
        )
    else:
        data_asset = data_source.get_asset(asset_name)

    batch_definition = data_asset.add_batch_definition_whole_table(name="batch_full_table")

    # 4. Criar a Suite de expectativas
    suite_name = "cosmetics_quality_suite"
    expectations_suite = gx.ExpectationSuite(suite_name)
    try:
        expectations_suite = context.suites.add(expectations_suite)
    except:
        expectations_suite = context.suites.get(suite_name)

    # --- Expectativas ---

    # --- 1. Coluna 'Name' não pode ser nula
    expectations_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Name")
    )

    # --- 2. Coluna 'Price' deve ser positiva
    expectations_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="Price", min_value=0)
    )

    # --- 3. Garantir que as colunas de metadados existam
    for col in ["_source_file", "_loaded_at"]:
        expectations_suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # 5. Validação e Checkpoint
    validation_def = gx.ValidationDefinition(
        data=batch_definition,
        suite=expectations_suite,
        name="cosmetics_validation"
    )

    checkpoint = gx.Checkpoint(
        name="glow_checkpoint",
        validation_definitions=[validation_def],
        actions=[gx.checkpoint.UpdateDataDocsAction(name="update_docs")],
        result_format={"result_format": "COMPLETE"}
    )

    # 6. Executar
    logger.info("Iniciando validação GX...")
    results = checkpoint.run()
    logger.info(f"Validação concluída ! Sucesso: {results.success}") 


if __name__ == "__main__":
    run_quality_checks()
