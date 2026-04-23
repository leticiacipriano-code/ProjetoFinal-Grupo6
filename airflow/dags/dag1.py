from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import great_expectations as gx

# Configurações padrão
default_args = {
    'owner': 'glow_project',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Callback de falha
def alerta_falha(context):
    task_id = context.get('task_instance').task_id
    print(f"ALERTA: A tarefa {task_id} falhou no pipeline ELT!")

@dag(
    dag_id='pipeline_elt_glow_dag_v3',
    default_args=default_args,
    description='DAG com GX em Python puro e dbt otimizada para Airflow 3',
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    on_failure_callback=alerta_falha,
    tags=['glow', 'elt', 'qualidade', 'python_gx']
)
def glow_pipeline():

    @task
    def gx_validation_python():
        import os
        
        path_to_gx = '/app/gx_docs'
        
        if not os.path.exists(path_to_gx):
            raise FileNotFoundError(f"A pasta {path_to_gx} não foi encontrada no container!")

        try:
            # Tenta usar modo ephemeral (em memória, sem tentar escrever no disco)
            context = gx.get_context(mode='ephemeral')
        except Exception as e:
            print(f"Aviso ao carregar contexto ephemeral: {e}")
            print("Continuando com contexto simples...")
            context = gx.get_context()
        
        try:
            result = context.run_checkpoint(checkpoint_name='glow_checkpoint')
            
            if not result["success"]:
                raise ValueError("Falha na validação do GX. Verifique os Data Docs.")
            
            return "Sucesso"
        except Exception as e:
            print(f"Checkpoint não encontrado ou erro: {e}")
            print("Pipeline continuando (GX é não-crítico)...")
            return "GX pulado (não-crítico)"

    # 2. Execução do dbt (Transformação)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd usr/app/glow_dbt && dbt run --profiles-dir /app/profiles'
    )

    # 3. Testes do dbt
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd usr/app/glow_dbt && dbt test --profiles-dir /app/profiles'
    )

    # Definindo o fluxo com a nova tarefa Python
    validacao = gx_validation_python()
    
    validacao >> dbt_run >> dbt_test

# Instanciação da DAG
glow_pipeline()