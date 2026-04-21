import great_expectations as gx
import logging
from validation.gx_config import create_gx_expectationSuite
from validation.expectation_validation_raw_cosmetics import generate_validation_raw_cosmetics
from validation.expectation_validation_raw_sales import generate_validation_raw_sales
from validation.expectation_validation_raw_products import generate_validation_raw_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_gx_validation():
    """Executa as validações do Great Expectations."""
    # Alteração: Usando um diretório padrão para o contexto de arquivo
    context = gx.get_context(mode="file", project_root_dir="/app/gx_docs")

    # 1. Definição do batch e suites
    batch_def_cosmetics, suite_cosmetics = create_gx_expectationSuite(context=context, asset="raw_cosmetics", table="cosmetics_products")
    batch_def_sales, suite_sales = create_gx_expectationSuite(context=context, asset="raw_sales", table="sales_data")
    batch_def_skincare, suite_skincare = create_gx_expectationSuite(context=context, asset="raw_skincare_products", table="skincare_products")

    # 2. Geração das Definições de Validação
    val_cosmetics = generate_validation_raw_cosmetics(batch_def_cosmetics, suite_cosmetics)
    val_sales = generate_validation_raw_sales(batch_def_sales, suite_sales)
    val_skincare = generate_validation_raw_products(batch_def_skincare, suite_skincare)

    validations = [val_cosmetics, val_sales, val_skincare]

    # 3. Registro OBRIGATÓRIO no contexto (GX 1.0+)
    # Usamos add_or_update para evitar o erro de "must be added"
    registered_validations = []
    for val in validations:
        try:
            # O segredo está em usar add_or_update para persistir no diretório do contexto
            registered_val = context.validation_definitions.add_or_update(val)
            registered_validations.append(registered_val)
        except Exception as e:
            logger.warning(f"Erro ao registrar validação {val.name}: {e}")

    # 4. Configuração das Ações (Update Data Docs)
    action_list = [
        gx.checkpoint.UpdateDataDocsAction(
            name="update_all_data_docs",
        ),
    ]

    # 5. Checkpoint
    checkpoint_name = "glow_checkpoint"
    checkpoint = gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=registered_validations, # Usamos as versões registradas
        actions=action_list,
        result_format={"result_format": "COMPLETE"}
    )

    try:
        context.checkpoints.add_or_update(checkpoint)
    except Exception as e:
        checkpoint = context.checkpoints.get(checkpoint_name)

    # 6. Executar e Gerar Docs
    logger.info("Iniciando validação GX...")
    results = checkpoint.run()
    
    # IMPORTANTE: Forçar a construção e pegar a URL para conferência
    context.build_data_docs()
    
    # Opcional: Logar onde o arquivo foi parar para você conferir no Windows
    logger.info(f"Validação concluída! Sucesso: {results.success}")
    logger.info(f"Data Docs gerados em: /app/gx_docs/uncommitted/data_docs/local_site/index.html")

if __name__ == "__main__":
    run_gx_validation()