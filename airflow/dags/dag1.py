from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from datetime import datetime, timedelta

# Configurações de tratamento de falhas (Retries e Alertas)
default_args = {
    'owner': 'glow_project',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # Tenta novamente 1 vez antes de falhar
    'retry_delay': timedelta(minutes=5), # Espera 5 minutos para o retry
}

def alerta_falha():
    print("ALERTA: Uma tarefa falhou no pipeline ELT!")

with DAG(
    'pipeline_elt_glow_dag',
    default_args=default_args,
    description='DAG com GX, dbt run e dbt test',
    schedule_interval=timedelta(days=1),
    catchup=False,
    on_failure_callback=alerta_falha # Alerta simples de falha
) as dag:

    # 1. Validação com Great Expectations
    gx_validate = GreatExpectationsOperator(
        task_id='gx_validation',
        conn_id='postgres_default', # ID da conexão configurada na UI do Airflow
        data_context_root_dir='/app/gx_docs', # Caminho dentro do container
        expectation_suite_name='glow_suite',
        return_json_dict=True
    )

    # 2. Execução do dbt (Transformação)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/app/dbt_project && dbt run --profiles-dir /root/.dbt'
    )

    # 3. Testes do dbt
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /usr/app/dbt_project && dbt test --profiles-dir /root/.dbt'
    )

    # Definindo a dependência explícita
    gx_validate >> dbt_run >> dbt_test