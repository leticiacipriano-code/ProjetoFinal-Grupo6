import great_expectations as gx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_validation_raw_products(batch, expectations):
    
    # --- Expectativas ---

    # --- 1. Coluna 'clean_ingreds' não pode ser nula
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="clean_ingreds", type_="TEXT")
    )

    # --- 2. Coluna 'price' deve conter o símbolo £
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(column="price", regex= r"^£\d+(\.\d{1,2})?$")
    )

    logger.info("=" * 60)
    logger.info(f"Definição do set de Expectations para skincare_products - {expectations}")

    # 5. Validação e Checkpoint
    validation_def = gx.ValidationDefinition(
        data=batch,
        suite=expectations,
        name="products_validation"
    )

    return validation_def


if __name__ == "__main__":
    generate_validation_raw_products()