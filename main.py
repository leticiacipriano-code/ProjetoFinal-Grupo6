from dotenv import load_dotenv
import logging
import sys
import subprocess
from pathlib import Path

# 1. Carregar as variáveis de ambiente
load_dotenv()

from bronze.ingest import get_engine, wait_for_table, run_ingestion
from validation.gx_run import run_gx_validation
from silver.silver import main as silver_main
from gold.gold import main as gold_main
from provisioning.metabase_setup import setup_metabase

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MAIN_PIPELINE")

def run_dbt_command(command_list, step_name):
    """Função genérica para executar comandos dbt usando o caminho fixo do Docker."""
    try:
        logger.info("-" * 40)
        logger.info(f"Executando: {step_name}")
        logger.info("-" * 40)
        
        # No Docker, a raiz é sempre /app
        # glow_dbt está em /app/glow_dbt
        # profiles está em /app/profiles
        
        dbt_working_dir = "/app/glow_dbt"
        profiles_path = "/app/profiles" # Caminho fixo para o container
        
        # 1. Verifica se a pasta do dbt existe
        if not Path(dbt_working_dir).exists():
            logger.error(f"Diretório dbt não encontrado em {dbt_working_dir}")
            return False

        # 2. Montamos o comando forçando o diretório de profiles para a raiz do container
        full_command = command_list + ["--profiles-dir", profiles_path]
        
        logger.info(f"Forçando Profiles para: {profiles_path}")
        
        result = subprocess.run(
            full_command,
            cwd=dbt_working_dir, # Define onde o comando 'nasce'
            text=True,
            capture_output=False 
        )
        
        if result.returncode != 0:
            logger.error(f"❌ {step_name} falhou com código {result.returncode}")
            return False
        
        logger.info(f"✓ {step_name} concluído com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao executar {step_name}: {e}")
        return False

def main():
    try:
        logger.info("=" * 80)
        logger.info("INICIANDO PIPELINE GLOW & CO - DATA ENGINEERING COMPLETO")
        logger.info("=" * 80)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 1: BRONZE LAYER (Extract & Load)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 1: BRONZE - Extração e Carregamento de Dados")
        logger.info("─" * 80)
        
        try:
            engine = get_engine()
            logger.info("✓ Conexão com banco de dados estabelecida")
            loaded_tables = run_ingestion(engine)
            logger.info(f"✓ {len(loaded_tables)} tabelas carregadas na camada Bronze")
            
            for table in loaded_tables:
                wait_for_table(engine, table)
            logger.info("✓ Todas as tabelas estão disponíveis no banco de dados")
            
        except Exception as e:
            logger.error(f"Falha na camada Bronze: {e}")
            sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 2: VALIDAÇÃO DE QUALIDADE (Great Expectations)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 2: VALIDAÇÃO - Great Expectations (GX)")
        logger.info("─" * 80)
        
        try:
            run_gx_validation()
            logger.info("✓ Validações de qualidade executadas com sucesso")
        except Exception as e:
            logger.error(f"Falha na validação GX: {e}")
            logger.warning("⚠ Pipeline continuando apesar do erro em GX...")

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 3: DBT TRANSFORMATIONS (Seed & Run)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 3: DBT - Carga de Seeds e Transformações")
        logger.info("─" * 80)

        # 3.1 dbt seed (Carrega os arquivos CSV da pasta seeds para o banco)
        seed_success = run_dbt_command(["dbt", "seed"], "dbt seed")
        if not seed_success:
            logger.warning("⚠ dbt seed falhou ou não carregou dados.")

        # 3.2 dbt run (Executa os modelos SQL)
        run_success = run_dbt_command(["dbt", "run"], "dbt run")
        if not run_success:
            logger.warning("⚠ dbt run falhou, os modelos podem estar incompletos.")

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 4: SILVER LAYER (Advanced Analytics)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 4: SILVER - Análise Avançada")
        logger.info("─" * 80)
        
        try:
            silver_main()
            logger.info("✓ Camada Silver processada")
        except Exception as e:
            logger.error(f"Falha na camada Silver: {e}")
            sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 5: GOLD LAYER (Business Analytics)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 5: GOLD - Camada Executiva")
        logger.info("─" * 80)
        
        try:
            gold_main()
            logger.info("✓ Camada Gold processada")
        except Exception as e:
            logger.error(f"Falha na camada Gold: {e}")
            sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 6: METABASE SETUP
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 6: METABASE - Setup de Dashboards")
        logger.info("─" * 80)
        
        try:
            setup_metabase()
            logger.info("✓ Metabase setup concluído")
        except Exception as e:
            logger.warning(f"⚠ Setup do Metabase falhou: {e}")

        # ─────────────────────────────────────────────────────────────────────────
        # SUCESSO!
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "=" * 80)
        logger.info("✓ PIPELINE GLOW & CO CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Falha crítica no pipeline: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()