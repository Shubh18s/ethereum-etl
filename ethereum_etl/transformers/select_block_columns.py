if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


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

    spark = kwargs.get('spark')
    # print(spark.sql('select 1'))

    block_columns = ['number', 'size', 'timestamp',
       'base_fee_per_gas', 'gas_limit', 'gas_used',
       'transaction_count', 'date', 'last_modified']
    
    df_block = data.select(block_columns) \
                .withColumnRenamed('date', 'block_date') \
                .withColumnRenamed('number', 'block_number') \
                .withColumnRenamed('timestamp', 'block_timestamp') \
                .withColumnRenamed('gas_limit', 'block_gas_limit') \
                .withColumnRenamed('base_fee_per_gas', 'block_base_fee_per_gas') \
                .withColumnRenamed('size', 'block_size') \
                .withColumnRenamed('gas_used', 'block_gas_used') \
                .withColumnRenamed('last_modified', 'block_last_modified') \
                .withColumnRenamed('transaction_count', 'block_transaction_count')

    return df_block


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
