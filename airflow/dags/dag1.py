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
        """
        Executa a validação do Great Expectations usando a biblioteca Python diretamente.
        """
        # Obtém o contexto de dados do diretório configurado
        context = gx.get_context(context_root_dir='/app/gx_docs')
        
        # Executa o checkpoint definido anteriormente
        result = context.run_checkpoint(checkpoint_name='glow_checkpoint')
        
        # Valida se o resultado foi bem sucedido
        if not result["success"]:
            # Você pode extrair detalhes específicos do erro aqui
            raise ValueError(f"Falha na validação de dados do GX. Verifique os logs do Checkpoint.")
        
        return "Dados validados com sucesso!"

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

    # Definindo o fluxo com a nova tarefa Python
    validacao = gx_validation_python()
    
    validacao >> dbt_run >> dbt_test

# Instanciação da DAG
glow_pipeline()