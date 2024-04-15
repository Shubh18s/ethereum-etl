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

    sc = spark.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", "./keys/google_credentials.json")
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    block_schema = types.StructType(
                        [
                            types.StructField('difficulty', types.DoubleType(), True), 
                            types.StructField('hash', types.StringType(), True), 
                            types.StructField('miner', types.StringType(), True), 
                            types.StructField('nonce', types.StringType(), True), 
                            types.StructField('number', types.LongType(), True), 
                            types.StructField('size', types.LongType(), True), 
                            types.StructField('timestamp', types.TimestampType(), True), 
                            types.StructField('total_difficulty', types.DoubleType(), True), 
                            types.StructField('base_fee_per_gas', types.LongType(), True), 
                            types.StructField('gas_limit', types.LongType(), True), 
                            types.StructField('gas_used', types.LongType(), True), 
                            types.StructField('extra_data', types.StringType(), True), 
                            types.StructField('logs_bloom', types.StringType(), True), 
                            types.StructField('parent_hash', types.StringType(), True), 
                            types.StructField('state_root', types.StringType(), True), 
                            types.StructField('receipts_root', types.StringType(), True), 
                            types.StructField('transactions_root', types.StringType(), True), 
                            types.StructField('sha3_uncles', types.StringType(), True), 
                            types.StructField('transaction_count', types.LongType(), True), 
                            types.StructField('date', types.StringType(), True), 
                            types.StructField('last_modified', types.TimestampType(), True)
                        ]
                    )

    return spark.read.schema(block_schema).parquet('gs://ethereum_etl_datalake/raw/eth/blocks/date=2024-04-08/')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
