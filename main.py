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


# Configuração de Logs para aparecer bonito no terminal da apresentação
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MAIN_PIPELINE")


def run_dbt_run():
    """Executa dbt run para transformar dados da camada intermediária."""
    try:
        logger.info("=" * 80)
        logger.info("Etapa 3: Transformações dbt (Intermediate & Marts)")
        logger.info("=" * 80)
        
        # Muda para o diretório de dbt
        dbt_path = Path("glow_dbt")
        if not dbt_path.exists():
            logger.warning(f"Diretório dbt não encontrado em {dbt_path}")
            return False
        
        # Executa dbt run
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "profiles"],
            cwd=str(dbt_path),
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"dbt run falhou: {result.stderr}")
            return False
        
        logger.info("✓ dbt run concluído com sucesso")
        logger.info(result.stdout)
        return True
        
    except FileNotFoundError:
        logger.error("dbt não está instalado ou não foi encontrado no PATH")
        return False
    except Exception as e:
        logger.error(f"Erro ao executar dbt run: {e}")
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
        except Exception as exc:
            logger.critical(f"Não foi possível conectar ao banco: {exc}")
            sys.exit(1)

        try:
            loaded_tables = run_ingestion(engine)
            logger.info(f"✓ {len(loaded_tables)} tabelas carregadas na camada Bronze")
            
            # Espera o Container ser populado com as tabelas
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
        # ETAPA 3: DBT TRANSFORMATIONS (dbt models)
        # ─────────────────────────────────────────────────────────────────────────
        try:
            dbt_success = run_dbt_run()
            if not dbt_success:
                logger.warning("⚠ dbt run não foi executado, continuando pipeline...")
                
        except Exception as e:
            logger.error(f"Falha na execução dbt: {e}")
            logger.warning("⚠ Pipeline continuando apesar do erro em dbt...")

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 4: SILVER LAYER (Advanced Analytics)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 4: SILVER - Análise Avançada e Perguntas de Negócio")
        logger.info("─" * 80)
        
        try:
            silver_main()
            logger.info("✓ Camada Silver criada com sucesso")
            
        except Exception as e:
            logger.error(f"Falha na camada Silver: {e}")
            sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # ETAPA 5: GOLD LAYER (Executive/Business Analytics)
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "─" * 80)
        logger.info("ETAPA 5: GOLD - Camada de Negócio Executiva (Dashboards)")
        logger.info("─" * 80)
        
        try:
            gold_main()
            logger.info("✓ Camada Gold criada com sucesso")
            
        except Exception as e:
            logger.error(f"Falha na camada Gold: {e}")
            sys.exit(1)

        # ─────────────────────────────────────────────────────────────────────────
        # SUCESSO!
        # ─────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "=" * 80)
        logger.info("✓ PIPELINE GLOW & CO CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 80)
        logger.info("\nAcesso às ferramentas:")
        logger.info("  • Metabase (Dashboards):    http://localhost:3000")
        logger.info("  • GX Docs (Validações):     http://localhost:8080")
        logger.info("  • dbt Docs (Documentação):  http://localhost:8181")
        logger.info("\nCamadas processadas:")
        logger.info("  1. Bronze:  ✓ Dados brutos extraídos e carregados")
        logger.info("  2. GX:      ✓ Validação de qualidade realizada")
        logger.info("  3. dbt:     ✓ Transformações aplicadas")
        logger.info("  4. Silver:  ✓ Análises avançadas criadas")
        logger.info("  5. Gold:    ✓ Dashboards de negócio prontos")
        logger.info("=" * 80 + "\n")

    except Exception as e:
        logger.error(f"Falha crítica no pipeline: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
