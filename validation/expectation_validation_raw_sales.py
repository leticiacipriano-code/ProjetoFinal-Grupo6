from datetime import date
import great_expectations as gx
from validation.gx_config import create_gx_expectationSuite


def generate_validation_raw_sales(batch, expectations):

    # --- Expectativas ---

    # --- 1. Coluna 'Boxes Shipped' deve ser do tipo inteiro
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="boxes_shipped", type_=int)
    )

    # --- 2. Coluna 'Price' deve ser positiva
    expectations.add_expectation(
        gx.expectations.ExpectColumnMaxToBeBetween(column="date", max_value=date.today())
    )


    # 5. Validação e Checkpoint
    validation_def = gx.ValidationDefinition(
        data=batch,
        suite=expectations,
        name="sales_validation"
    )

    return validation_def