import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder\
        .appName('transform_schema') \
        .getOrCreate()

transaction_schema = types.StructType(
        [
            types.StructField('gas', types.IntegerType(), True), 
            types.StructField('hash', types.StringType(), True), 
            types.StructField('input', types.StringType(), True), 
            types.StructField('nonce', types.IntegerType(), True), 
            types.StructField('value', types.DoubleType(), True), 
            types.StructField('block_number', types.IntegerType(), True), 
            types.StructField('block_hash', types.StringType(), True), 
            types.StructField('transaction_index', types.IntegerType(), True), 
            types.StructField('from_address', types.StringType(), True), 
            types.StructField('to_address', types.StringType(), True), 
            types.StructField('gas_price', types.IntegerType(), True), 
            types.StructField('receipt_cumulative_gas_used', types.IntegerType(), True), 
            types.StructField('receipt_gas_used', types.IntegerType(), True), 
            types.StructField('receipt_contract_address', types.StringType(), True), 
            types.StructField('receipt_status', types.IntegerType(), True), 
            types.StructField('receipt_effective_gas_price', types.IntegerType(), True), 
            types.StructField('transaction_type', types.IntegerType(), True), 
            types.StructField('max_fee_per_gas', types.IntegerType(), True), 
            types.StructField('max_priority_fee_per_gas', types.IntegerType(), True), 
            types.StructField('block_timestamp', types.TimestampType(), True), 
            types.StructField('date', types.DateType(), True), 
            types.StructField('last_modified', types.TimestampType(), True)
        ]
    )

df_transactions = spark.read \
                .option("header", "true") \
                .schema(transaction_schema) \
                .parquet('raw/eth/transactions/*/*')

df_transactions \
    .repartition(4) \
    .write.parquet('data/eth/transactions', mode='overwrite')