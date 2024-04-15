from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os

@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext

    from pyspark.sql import types

    spark = kwargs.get('spark')
    
    spark_conf = spark.sparkContext.getConf()
    # spark_conf \
    #     .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    #     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    #     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "./keys/google_credentials.json")

    sc = spark.sparkContext
    # sc = spark.sparkContext(conf=spark_conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", "./keys/google_credentials.json")
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    # bucket_name = os.getenv('BUCKET_NAME')
    # object_key = "raw/eth/transactions/date=2024-04-08/part-00000-4c2fde61-a460-4d0e-bfc1-07acff938569-c000.snappy.parquet"

    # df_transactions = spark.read.parquet('gs://ethereum_etl_datalake/raw/eth/transactions/')
    

    transaction_schema = types.StructType(
        [
            types.StructField('gas', types.LongType(), True), 
            types.StructField('hash', types.StringType(), True), 
            types.StructField('input', types.StringType(), True), 
            types.StructField('nonce', types.LongType(), True), 
            types.StructField('value', types.DoubleType(), True), 
            types.StructField('block_number', types.LongType(), True), 
            types.StructField('block_hash', types.StringType(), True), 
            types.StructField('transaction_index', types.LongType(), True), 
            types.StructField('from_address', types.StringType(), True), 
            types.StructField('to_address', types.StringType(), True), 
            types.StructField('gas_price', types.LongType(), True), 
            types.StructField('receipt_cumulative_gas_used', types.LongType(), True), 
            types.StructField('receipt_gas_used', types.LongType(), True), 
            types.StructField('receipt_contract_address', types.StringType(), True), 
            types.StructField('receipt_status', types.LongType(), True), 
            types.StructField('receipt_effective_gas_price', types.LongType(), True), 
            types.StructField('transaction_type', types.LongType(), True), 
            types.StructField('max_fee_per_gas', types.LongType(), True), 
            types.StructField('max_priority_fee_per_gas', types.LongType(), True), 
            types.StructField('block_timestamp', types.TimestampType(), True), 
            types.StructField('date', types.StringType(), True), 
            types.StructField('last_modified', types.TimestampType(), True)
        ]
    )


    return spark.read.schema(transaction_schema).parquet('gs://ethereum_etl_datalake/raw/eth/transactions/date=2024-04-08/')
    # print(df_transactions.count())
    # return df_transactions

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
