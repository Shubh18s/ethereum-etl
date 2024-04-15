if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pyspark.sql.functions import col, isnan, when, trim

def to_null(c):
    return when(~(col(c).isNull() | (trim(col(c)) == "")), col(c))

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

    transaction_columns = ['gas', 'nonce', 'value', 'block_number', 'gas_price',
       'receipt_cumulative_gas_used', 'receipt_gas_used', 'receipt_status',
       'receipt_effective_gas_price', 'transaction_type', 'max_fee_per_gas',
       'max_priority_fee_per_gas', 'block_timestamp', 'date', 'last_modified']
    
    df_transaction = data.select(transaction_columns).withColumnRenamed('date', 'block_date')
    
    df_transaction.select([to_null(c).alias(c) for c in df_transaction.columns]).na.drop()

    return df_transaction
    # return df_transaction


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
