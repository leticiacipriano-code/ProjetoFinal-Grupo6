import great_expectations as gx
from validation.gx_config import create_gx_expectationSuite


def generate_validation_raw_products(batch, expectations):
    
    # --- Expectativas ---

    # --- 1. Coluna 'clean_ingreds' não pode ser nula
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="clean_ingreds", type_=str)
    )

    # --- 2. Coluna 'price' deve conter o símbolo £
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(column="price", regex= r"^£\d+(\.\d{1,2})?$")
    )

    # 5. Validação e Checkpoint
    validation_def = gx.ValidationDefinition(
        data=batch,
        suite=expectations,
        name="products_validation"
    )

    return validation_def