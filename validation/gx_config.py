import great_expectations as gx
import os
from sqlalchemy import create_engine


def create_gx_expectationSuite(asset: str, table: str):
    
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

    asset_name = asset
    if asset_name not in [asset.name for asset in data_source.assets]:
        data_asset = data_source.add_table_asset(
            name=asset_name,
            table_name=table,
            schema_name="raw"
        )
    else:
        data_asset = data_source.get_asset(asset_name)

    try:
        batch_definition = data_asset.get_batch_definition("batch_full_table")
    except:
        batch_definition = data_asset.add_batch_definition_whole_table(name="batch_full_table")

    # 4. Criar a Suite de expectativas
    suite_name = f"{asset}_quality_suite"
    expectations_suite = gx.ExpectationSuite(suite_name)
    try:
        expectations_suite = context.suites.add(expectations_suite)
    except:
        expectations_suite = context.suites.get(suite_name)

    return batch_definition, expectations_suite


if __name__ == "__main__":
    create_gx_expectationSuite()
