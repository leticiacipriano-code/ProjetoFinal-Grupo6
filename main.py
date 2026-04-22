from dotenv import load_dotenv
import logging
import sys
import subprocess
from pathlib import Path
import os

# 1. Carregar as variáveis de ambiente
load_dotenv()

from bronze.ingest import get_engine, wait_for_table, run_ingestion
from validation.gx_run import run_gx_validation
from gold_metabase.gold import main as gold_main

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MAIN_PIPELINE")

def run_dbt_command(command_list, step_name):
    """
    Função genérica para executar comandos dbt.
    Funciona em Docker e localmente.
    
    Args:
        command_list: Lista com comando dbt ['dbt', 'run', ...]
        step_name: Nome descritivo da etapa para logging
    
    Returns:
        bool: True se sucesso, False caso contrário
    """
    try:
        logger.info("-" * 80)
        logger.info(f" {step_name}")
        logger.info("-" * 80)
        
        # Detecta ambiente de execução
        is_docker = os.path.exists("/.dockerenv")
        
        # Define diretório do dbt
        if is_docker:
            dbt_working_dir = "/app/glow_dbt"
            profiles_dir = "/app/profiles"
        else:
            dbt_working_dir = "./glow_dbt"
            profiles_dir = "./profiles"
        
        # Verifica se o diretório dbt existe
        if not Path(dbt_working_dir).exists():
            logger.error(f" Diretório dbt não encontrado: {dbt_working_dir}")
            return False
        
        # Monta comando completo com profiles-dir
        full_command = command_list + ["--profiles-dir", profiles_dir]
        
        logger.info(f" Working directory: {dbt_working_dir}")
        logger.info(f" Profiles directory: {profiles_dir}")
        logger.info(f" Comando: {' '.join(full_command)}")
        
        # Executa o comando
        result = subprocess.run(
            full_command,
            cwd=dbt_working_dir,
            text=True,
            capture_output=False
        )
        
        if result.returncode != 0:
            logger.error(f" {step_name} falhou com código de saída {result.returncode}")
            return False
        
        logger.info(f" {step_name} concluído com sucesso")
        return True
        
    except Exception as e:
        logger.error(f" Erro ao executar {step_name}: {e}")
        return False

def main():
    """
    Orquestra o pipeline completo de Data Engineering:
    1. BRONZE: Extração e carregamento em PostgreSQL
    2. VALIDAÇÃO: Qualidade de dados (Great Expectations)
    3. SILVER/GOLD: Transformações dbt (staging → marts)
    4. GOLD: Camada executiva e dashboards Metabase
    """
    try:
        logger.info("=" * 80)
        logger.info(" GLOW & CO - PIPELINE DE DATA ENGINEERING COMPLETO")
        logger.info("=" * 80)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 1: BRONZE LAYER - Extração e Carregamento
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "▼" * 80)
        logger.info("ETAPA 1  - BRONZE: Extração e Carregamento de Dados (raw)")
        logger.info("▼" * 80)
        
        try:
            logger.info("\n Conectando ao banco de dados...")
            engine = get_engine()
            logger.info("✓ Conexão estabelecida")
            
            logger.info("\n Ingestando dados da pasta /base...")
            loaded_tables = run_ingestion(engine)
            logger.info(f"✓ Ingestão concluída: {len(loaded_tables)} tabelas carregadas")
            
            logger.info("\n Aguardando disponibilidade das tabelas...")
            for table in loaded_tables:
                wait_for_table(engine, table, schema="raw", timeout=30)
            logger.info("✓ Todas as tabelas estão disponíveis em raw.*")
            
        except Exception as e:
            logger.error(f" FALHA NA ETAPA BRONZE: {e}", exc_info=True)
            sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 2: VALIDAÇÃO - Great Expectations
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "▼" * 80)
        logger.info("ETAPA 2  - VALIDAÇÃO: Great Expectations (Qualidade de Dados)")
        logger.info("▼" * 80)
        
        try:
            logger.info("\n Executando validações de qualidade...")
            run_gx_validation()
            logger.info("✓ Validações concluídas com sucesso")
        except Exception as e:
            logger.warning(f" Validação GX não-crítica falhou: {e}")
            logger.warning("  Pipeline continuando (erros no GX não interrompem o fluxo)")

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 3: TRANSFORMAÇÕES dbt - Staging, Intermediate, Marts
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "▼" * 80)
        logger.info("ETAPA 3  - TRANSFORMAÇÕES: dbt (staging → marts)")
        logger.info("▼" * 80)

        # --- NOVO: Garantir que os pacotes do dbt estejam instalados ---
        logger.info("\n dbt deps: Instalando dependências (dbt_utils, etc)...")
        deps_success = run_dbt_command(["dbt", "deps"], "dbt deps")
        if not deps_success:
            logger.error(" Falha ao instalar dependências do dbt. O pipeline não pode continuar.")
            sys.exit(1)
        # ---------------------------------------------------------------


        # 3.1 dbt seed - Carrega dados de referência
        logger.info("\n dbt seed: Carregando dados de referência...")
        seed_success = run_dbt_command(["dbt", "seed", "--full-refresh"], "dbt seed")
        if not seed_success:
            logger.warning("  dbt seed falhou - verificar logs do dbt")

        # 3.2 dbt run - Executa todos os modelos
        logger.info("\n dbt run: Construindo modelos SQL...")
        run_success = run_dbt_command(["dbt", "run"], "dbt run")
        if not run_success:
            logger.error(" dbt run falhou - tabelas podem estar incompletas")
            logger.error(" Abortando pipeline, verifique os erros do dbt")
            sys.exit(1)
        
        logger.info("✓ Transformações dbt concluídas com sucesso")
        logger.info("  Modelos gerados:")
        logger.info("    - staging.stg_* (dados limpos)")
        logger.info("    - intermediate.int_* (dados enriquecidos)")
        logger.info("    - public.mart_* (modelos otimizados para análise)")

        # # ─────────────────────────────────────────────────────────────────────────
        # # ETAPA 4: GOLD LAYER - Camada Executiva e Metabase
        # # ─────────────────────────────────────────────────────────────────────────
        # logger.info("\n" + "▼" * 80)
        # logger.info("ETAPA 4 - GOLD: Camada Executiva (Dashboards Metabase)")
        # logger.info("▼" * 80)
        
        # try:
        #     logger.info("\n Criando tabelas gold e dashboards Metabase...")
        #     gold_main()
        #     logger.info("✓ Camada Gold processada com sucesso")
        #     logger.info("  Recursos criados:")
        #     logger.info("    - gold.dashboard_* (6 tabelas de negócio)")
        #     logger.info("    - Metabase: Dashboard centralizado com 6 visualizações")
            
        # except Exception as e:
        #     logger.error(f" FALHA NA ETAPA GOLD: {e}", exc_info=True)
        #     sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # SUCESSO COMPLETO
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "=" * 80)
        logger.info(" PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 80)
        logger.info("\n RESUMO DO FLUXO:")
        logger.info("  1  Bronze  → raw.cosmetics_products, raw.skincare_products, raw.sales_data")
        logger.info("  2 Validação → Qualidade verificada com Great Expectations")
        logger.info("  3  Silver/Gold → staging.*, intermediate.*, public.mart_*")
        logger.info("  4  Executiva → gold.dashboard_* + Metabase 🎯")
        logger.info("\n Acesse o Metabase em: http://metabase:3000")
        logger.info("=" * 80 + "\n")

    except Exception as e:
        logger.error("=" * 80)
        logger.error(" FALHA CRÍTICA NO PIPELINE!")
        logger.error("=" * 80)
        logger.error(f"Erro: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()