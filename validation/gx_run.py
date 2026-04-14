import great_expectations as gx
import logging
from validation.gx_config import create_gx_expectationSuite
from validation.expectation_validation_raw_cosmetics import generate_validation_raw_cosmetics
from validation.expectation_validation_raw_sales import generate_validation_raw_sales
from validation.expectation_validation_raw_products import generate_validation_raw_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_gx_validation():

    # Definição do batch_definition e expectation_suite para cada tabela validada
    batch_definition_cosmetics, expectations_suite_cosmetics = create_gx_expectationSuite(asset="raw_cosmetics", table="cosmetics_products")
    batch_definition_sales, expectations_suite_sales = create_gx_expectationSuite(asset="raw_sales", table="sales_data")
    batch_definition_skincare, expectations_suite_skincare = create_gx_expectationSuite(asset="raw_skincare_products", table="skincare_products")

    # Execução das Validações
    validation_def_cosmetics = generate_validation_raw_cosmetics(batch_definition_cosmetics, expectations_suite_cosmetics)
    validation_def_sales = generate_validation_raw_sales(batch_definition_sales, expectations_suite_sales)
    validation_def_skincare = generate_validation_raw_products(batch_definition_skincare, expectations_suite_skincare)

    # Criação da lista para passar pro checkpoint
    validations = [validation_def_cosmetics, validation_def_sales, validation_def_skincare]

    context = gx.get_context(mode="file", project_root_dir="/app/gx_docs")

    checkpoint = gx.Checkpoint(
        name="glow_checkpoint",
        validation_definitions=validations,
        actions=[gx.checkpoint.UpdateDataDocsAction(name="update_docs")],
        result_format={"result_format": "COMPLETE"}
    )

    context.checkpoints.add(checkpoint)

    # 6. Executar
    logger.info("Iniciando validação GX...")
    results = checkpoint.run()
    logger.info(f"Validação concluída ! Sucesso: {results.success}")