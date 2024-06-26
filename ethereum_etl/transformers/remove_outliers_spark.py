import os
from pyspark.sql import SparkSession

from pyspark.sql import types

spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()
# print(spark.sql('select 1'))


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
    transaction_df = spark.read.parquet('./raw/eth/transactions/date=2024-04-02/')
    
    transaction_cols = ['gas', 'nonce', 'value', 'block_number', 'gas_price',
       'receipt_cumulative_gas_used', 'receipt_gas_used', 'receipt_status',
       'receipt_effective_gas_price', 'transaction_type', 'max_fee_per_gas',
       'max_priority_fee_per_gas', 'block_timestamp', 'date', 'last_modified']
    
    transaction_schema = types.StructType(
        [
            types.StructField('difficulty', types.DoubleType(), True), 
            types.StructField('hash', types.StringType(), True), 
            types.StructField('miner', types.StringType(), True), 
            types.StructField('nonce', types.StringType(), True), 
            types.StructField('number', types.IntegerType(), True), 
            types.StructField('size', types.IntegerType(), True), 
            types.StructField('timestamp', types.TimestampType(), True), 
            types.StructField('total_difficulty', types.DoubleType(), True), 
            types.StructField('base_fee_per_gas', types.IntegerType(), True), 
            types.StructField('gas_limit', types.IntegerType(), True), 
            types.StructField('gas_used', types.IntegerType(), True), 
            types.StructField('extra_data', types.StringType(), True), 
            types.StructField('logs_bloom', types.StringType(), True), 
            types.StructField('parent_hash', types.StringType(), True), 
            types.StructField('state_root', types.StringType(), True), 
            types.StructField('receipts_root', types.StringType(), True), 
            types.StructField('transactions_root', types.StringType(), True), 
            types.StructField('sha3_uncles', types.StringType(), True), 
            types.StructField('transaction_count', types.IntegerType(), True), 
            types.StructField('date', types.StringType(), True), 
            types.StructField('last_modified', types.TimestampType(), True)
        ]
    )

    transaction_df = transaction_df.select(transaction_cols)

    return transaction_df.toPandas()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
