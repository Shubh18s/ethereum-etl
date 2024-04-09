#!/usr/bin/env python
# coding: utf-8

import os, argparse
import boto3
import pandas as pd
from botocore import UNSIGNED, exceptions
from botocore.client import Config

import datetime as dt
from datetime import date, timedelta



bucket_name =  'aws-public-blockchain'
blocks_prefix = 'v1.0/eth/blocks/date='
transactions_prefix = 'v1.0/eth/transactions/date='

blocks_output = 'raw/eth/blocks'
transaction_output = 'raw/eth/transactions'



def prepare_object_prefix(run_date:date):
    print(run_date)
    if not run_date:
        run_date = dt.date.today()

    start_date = run_date - dt.timedelta(days=7)
    end_date = run_date - dt.timedelta(days=0)
    
    block_objs = []
    transaction_objs = []
    
    for n in range(int((end_date - start_date).days)):
        
        date_prefix = start_date + timedelta(n)
        
        block_obj_prefix = f"{blocks_prefix}{date_prefix}/"
        transaction_obj_prefix = f"{transactions_prefix}{date_prefix}/"
        
        block_objs.append(block_obj_prefix)
        transaction_objs.append(transaction_obj_prefix)
    
    return(block_objs, transaction_objs)
        

def download_and_verify(Bucket, Key, Filename):
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    try:
        # os.remove(Filename)
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        s3_client.download_file(Bucket,Key,Filename)
        return os.path.exists(Filename)
    except exceptions.ClientError as error:
        print(error.response['Error']['Code']) #a summary of what went wrong
        print(error.response['Error']['Message']) #explanation of what went wrong
        return False


def download_raw_data(block_files=[], transaction_files=[], outpath:str='raw/eth/'):
    
    s3_resource = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3_resource.Bucket(bucket_name)
    
    for file_loc in block_files:
        
        bucket_obj = bucket.objects.filter(Prefix=file_loc)
        fdate = file_loc.split('/')[3]
        output_path = f"{blocks_output}/{fdate}/"
        
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
        
        for item in bucket_obj:       
            file_name = f"{output_path}{item.key.split('/')[-1]}"
            
            if not os.path.isfile(file_name):
                download_and_verify(bucket_name, item.key, file_name)
                
    
    for file_loc in transaction_files:
        
        bucket_obj = bucket.objects.filter(Prefix=file_loc)
        fdate = file_loc.split('/')[3]
        output_path = f"{transaction_output}/{fdate}/"
        
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
        
        for item in bucket_obj:       
            file_name = f"{output_path}{item.key.split('/')[-1]}"
            
            if not os.path.isfile(file_name):
                download_and_verify(bucket_name, item.key, file_name)

def main(params):
    blocks, transactions = prepare_object_prefix(params.run_date)
    download_raw_data(blocks, transactions)


if __name__ == '__main__':
    #Parsing arguments 
    parser = argparse.ArgumentParser(description='Loading data from s3 bucket to datalake.')

    
    parser.add_argument(
        '--run_date',
        type=lambda s: dt.strftime(s, "%Y-%m-%d"),
        help='run date to get data week ending to run_date.'
    )
    args = parser.parse_args()
    main(args)
