from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.great_expectations.operators.great_expectations import GreatExpectationsOperator
from datetime import datetime, timedelta
import logging

# Função simples para alerta de falha (pode ser expandida para Slack/Email)
def on_failure_callback(context):
    exception = context.get('exception')
    task_id = context.get('task_instance').task_id
    logging.error(f"A tarefa {task_id} falhou. Erro: {exception}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
}

with DAG(
    'pipeline_dados_dbt_ge',
    default_args=default_args,
    description='Pipeline com Great Expectations e dbt',
    schedule_interval='@daily',
    catchup=False,
    tags=['engenharia_de_dados', 'dbt', 'gx'],
) as dag:

    # 1. Validar dados brutos com Great Expectations
    validate_raw_data = GreatExpectationsOperator(
        task_id='validate_raw_data',
        expectation_suite_name='my_suite',
        data_asset_name='my_raw_table',
        execution_engine_conn_id='my_database_conn',
        # O checkpoint deve estar configurado no seu projeto GX
        checkpoint_name='my_checkpoint', 
    )

    # 2. Executar transformações com dbt run
    # Assume que o diretório do projeto dbt está acessível
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /caminho/do/projeto/dbt && dbt run --profiles-dir .',
    )

    # 3. Executar testes de integridade do dbt
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /caminho/do/projeto/dbt && dbt test --profiles-dir .',
    )

    # Fluxo de Dependências
    validate_raw_data >> dbt_run >> dbt_test