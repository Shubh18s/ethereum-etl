# Ethereum-ETL
Ethereum ETL lets you process and ingest daily ethereum blocks and transaction data and into a DataLake (GCS) and Data Warehouse (BigQuery).

![alt text](https://github.com/Shubh18s/ethereum-etl/blob/main/images/ethereum_etl_infra.jpg)

## Usecase
The ethereum network usage continues to grow with roughly 1 million transactions executed every day. This includes the normal payment transfers as well as the smart contracts being executed on the ethereum network. This increase in on-chain data has led to a higher usage of Gas -- the computational resources or effort required to execute transactions on the ethereum network.

The payment for the gas required to execute a transaction has to be made by the sender. This payment or Gas fees needs to include both a base fee needed to execute transaction on the chain plus a priority fee or tip for the validator who will execute the transaction and broadcast the resulting state of the Ethereum Virtual Machine (EVM) to the network.

With a lot of senders trying to get their transactions executed, the priority fee acts as an incentive for the validator to include their transaction on a block (a batch of transactions) rather than mining empty blocks.
However, this adds additional problems with senders needing to compete to have their transactions on the block. While there are measures in place such as maxFee or maxPriorityFee at the block level, the demand of the ethereum network has led to users offering higher tip amounts to outbid others.

## The Project
While there have been initiatives to reduce gas costs, monitoring gas fees and analysis of the historical transaction data is essential. The Ethereum-ETL pipeline helps you easily and efficiently ingest ethereum transactions and blocks data into Google Cloud Storage Data Lake and Google BigQuery Data Warehouse for further analysis. It also features custom transformations in DBT to further analyse and report the gas usage and prices.
<!-- 
Tools such as Etherscan and other Gas Estimators have been in market to guide users on average Gas prices.  -->

Read more about introduction to ethereum here - https://ethereum.org/en/developers/docs/intro-to-ethereum/
Using Spark

## Data Source
Ethereum-ETL uses the registry of open data on AWS (https://registry.opendata.aws/) to extract data for ethereum specific blocks and transactions from https://registry.opendata.aws/aws-public-blockchain/


## Architecture

With roughly a million transactions flowing through the ethereum network everyday, it was paramount to use Big Data technologies for fast and efficient data processsing and etl throughput. Ethereum-ETL uses Spark to extract transform and load ethereum data in parquet file format.

Orchestrator - I used Mage (https://www.mage.ai/) to create a custom Spark Session at the Project Level. This allowed for running spark jobs within all the pipeline blocks without the need to create spark session every time we execute the next block.

Another advantage was that I could provide the spark_config and spark jars as config in the metadata.yaml to connect to GCS.

Data Lake - Google Cloud Storage

Data Warehouse - Google Bigquery

Data Transformation for Analysis - DBT

Data Visualisation - Tableu

## How to run

### Running ethereum_etl pipeline
1. Create a GCP service account with below roles and generate credentials as json.
    - Create Service Accounts
    - Service Account User
    - Service Usage Admin
    - Storage Admin
    - Storage Object Admin

2. Create a Google Cloud Storage bucket - ```'ethereum_etl_datalake'```

3. Create a Google Cloud Bigquery dataset - ```'ethereum_master'```

4. Save the google credentials in a file on machine and run below commands in terminal - 

    ```export PATH_TO_GOOGLE_CREDENTIALS='PATH/TO/KEYS/FOLDER/'```

    ```export GOOGLE_PROJECT_ID='<YOUR-PROJECT-ID>'```

    ```export BUCKET_NAME='<BUCKET-NAME>'```

5. Update the RUN_DATE and DAYS_TO_TAKE parameter in dev.env file and run command - 

    ```cp dev.env .env```

    ```RUN_DATE``` = Date for which you want to run pipeline. Default is today's date. Data will be ingested to 2 days before RUN_DATE.
    ```DAYS_TO_TAKE``` = Number of days to ingest data for. Default is 1.

6. Update GOOGLE_SERVICE_ACC_KEY_FILEPATH in ethereum_etl/io_config.yaml with the google_credentials file name.

7. Open localhost:6789 for Mage UI and run pipeline once using pipeline trigger -

![alt text](https://github.com/Shubh18s/ethereum-etl/blob/main/images/pipeline_trigger.png)

![alt text](https://github.com/Shubh18s/ethereum-etl/blob/main/images/mage_pipeline.png)

### Running DBT transformations

1. Install Pipenv using -
    ```pip install pipenv```

2. Goto dbt directory and update dev/profiles.yml with the correct project, dataset and keyfile.

3. To check connection from dbt directory run -
    ```dbt debug --profiles-dir ./dev --project-dir ./ethereum_transformation/```

4. Run -
    ```dbt build --profiles-dir ./dev --project-dir ./ethereum_transformation/```

### Visualisations

![alt text](https://github.com/Shubh18s/ethereum-etl/blob/main/images/visualizations_tableau.png)

<!-- ## Next steps
DataProc
Deployment to Cloud Run -->

## Acknowledgments and Guidance

- https://ethereum-etl.readthedocs.io/en/latest/
- https://github.com/aws-solutions-library-samples/guidance-for-digital-assets-on-aws/tree/main


# Developer

### Shubhdeep Singh (singh18shubhdeep@gmail.com)
### [LinkedIn](https://www.linkedin.com/in/shubh18s/)