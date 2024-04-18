if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pyspark.sql.functions import col, udf

@udf
def convert_wei_to_gwei(amount):
    return amount/(10**9)

@udf
def convert_wei_to_eth(amount):
    return amount/(10**18)

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    from pyspark.sql import functions as F
    from pyspark.sql import types

    df = data \
            .withColumn('value_gwei', convert_wei_to_gwei("value").cast(types.FloatType())) \
            .withColumn('max_fee_per_gas_gwei', convert_wei_to_gwei("max_fee_per_gas").cast(types.FloatType())) \
            .withColumn('max_priority_fee_per_gas_gwei', convert_wei_to_gwei("max_priority_fee_per_gas").cast(types.FloatType())) \
            .withColumn('receipt_effective_gas_price_gwei', convert_wei_to_gwei("receipt_effective_gas_price").cast(types.FloatType())) \
            .withColumn('gas_price_gwei', convert_wei_to_gwei("gas_price").cast(types.FloatType()))
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
