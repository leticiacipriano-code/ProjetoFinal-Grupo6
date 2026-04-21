from datetime import date
import great_expectations as gx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_validation_raw_sales(batch, expectations):

    # --- Expectativas ---

    # --- 1. Coluna 'Boxes Shipped' deve ser do tipo inteiro
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="boxes_shipped", type_="INTEGER")
    )

    # --- 2. Coluna 'Price' deve ser positiva
    expectations.add_expectation(
        gx.expectations.ExpectColumnMaxToBeBetween(column="date", max_value=date.today())
    )

    logger.info("=" * 60)
    logger.info(f"Definição do set de Expectations para sales_data - {expectations}")

    # 5. Validação e Checkpoint
    validation_def = gx.ValidationDefinition(
        data=batch,
        suite=expectations,
        name="sales_validation"
    )

    return validation_def


if __name__ == "__main__":
    generate_validation_raw_sales()