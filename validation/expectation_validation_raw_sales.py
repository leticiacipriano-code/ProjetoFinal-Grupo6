from datetime import date
import great_expectations as gx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_validation_raw_sales(batch, expectations):

    # --- Expectativas ---

    # --- 1. Coluna 'Boxes Shipped' deve ser do tipo inteiro
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="boxes_shipped", type_="int64")
    )

    # --- 2. Coluna 'Date' deve existir e ser do tipo date
    expectations.add_expectation(
        gx.expectations.ExpectColumnToExist(column="date")
    )
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="date", type_="date")
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