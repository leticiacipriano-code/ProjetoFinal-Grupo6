import great_expectations as gx
from validation.gx_config import create_gx_expectationSuite


def generate_validation_raw_cosmetics(batch, expectations):
    
    # --- Expectativas ---

    # --- 1. Coluna 'Name' não pode ser nula
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="name")
    )

    # --- 2. Coluna 'Price' deve ser positiva
    expectations.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0)
    )

    # --- 3. Garantir que as colunas de metadados existam
    for col in ["_source_file", "_loaded_at"]:
        expectations.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # 5. Validação e Checkpoint
    validation_def = gx.ValidationDefinition(
        data=batch,
        suite=expectations,
        name="cosmetics_validation"
    )

    return validation_def