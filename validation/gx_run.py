import great_expectations as gx
import logging
from validation.gx_config import create_gx_expectationSuite
from validation.expectation_validation_raw_cosmetics import generate_validation_raw_cosmetics
from validation.expectation_validation_raw_sales import generate_validation_raw_sales
from validation.expectation_validation_raw_products import generate_validation_raw_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_gx_validation():
    """Executa as validações do Great Expectations (versão leve para performance)."""
    try:
        logger.info("Iniciando configuração de Great Expectations...")
        
        # Alteração: Usando um diretório padrão para o contexto de arquivo
        context = gx.get_context(mode="file", project_root_dir="/app/gx_docs")

        # 1. Definição do batch e suites (sem executar - apenas setup)
        batch_def_cosmetics, suite_cosmetics = create_gx_expectationSuite(context=context, asset="raw_cosmetics", table="cosmetics_products")
        batch_def_sales, suite_sales = create_gx_expectationSuite(context=context, asset="raw_sales", table="sales_data")
        batch_def_skincare, suite_skincare = create_gx_expectationSuite(context=context, asset="raw_skincare_products", table="skincare_products")

        # 2. Geração das Definições de Validação
        val_cosmetics = generate_validation_raw_cosmetics(batch_def_cosmetics, suite_cosmetics)
        val_sales = generate_validation_raw_sales(batch_def_sales, suite_sales)
        val_skincare = generate_validation_raw_products(batch_def_skincare, suite_skincare)

        validations = [val_cosmetics, val_sales, val_skincare]

        # 3. Registro OBRIGATÓRIO no contexto (GX 1.0+)
        registered_validations = []
        for val in validations:
            try:
                registered_val = context.validation_definitions.add_or_update(val)
                registered_validations.append(registered_val)
                logger.info(f"✓ Validação '{val.name}' registrada")
            except Exception as e:
                logger.warning(f"Erro ao registrar validação {val.name}: {e}")

        # 4. Definir e Rodar o Checkpoint (Gera os resultados para o relatório)
        checkpoint = gx.Checkpoint(
            name="glow_checkpoint",
            validation_definitions=registered_validations,
            result_format="SUMMARY"
        )
        context.checkpoints.add_or_update(checkpoint)
        
        logger.info("🚀 Executando validações reais no banco de dados...")
        result = checkpoint.run() # Executa a query SQL e testa as regras
        
        # 5. Agora sim, build data docs (Com os resultados do 'run')
        context.build_data_docs()
        
    except Exception as e:
        logger.error(f"Erro ao configurar GX: {e}")
        raise

if __name__ == "__main__":
    run_gx_validation()