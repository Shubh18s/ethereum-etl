from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os

import datetime as dt

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = os.getenv('BUCKET_NAME')

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


    run_date = dt.date.today()
    if(os.getenv('RUN_DATE') and os.getenv('RUN_DATE')!=''):
        run_date = os.getenv('RUN_DATE')        
        run_date = dt.datetime.strptime(run_date, '%Y-%m-%d').date()

    bucket_name = os.getenv('BUCKET_NAME')

    return df \
        .repartition(7) \
        .write \
        .parquet(f"gs://{bucket_name}/run_date={run_date}/processed/eth/blocks/", mode='overwrite')
