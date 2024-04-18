# Ethereum ETL
Ethereum ETL lets you process and ingest daily ethereum blocks and transaction data and into a DataLake (GCS) and Data Warehouse (BigQuery).

# Usecase
The ethereum network usage continues to grow with roughly 1 million transactions executed every day. This includes the normal payment transfers as well as the smart contracts being executed on the ethereum network. This increase in on-chain data has led to a higher usage of Gas -- the computational resources or effort required to execute transactions on the ethereum network.

The payment for the gas required to execute a transaction has to be made by the sender. This payment or Gas fees needs to include both a base fee needed to execute transaction on the chain plus a priority fee or tip for the validator who will execute the transaction and broadcast the resulting state of the Ethereum Virtual Machine (EVM) to the network.

With a lot of senders trying to get their transactions executed, the priority fee acts as an incentive for the validator to include their transaction on a block (a batch of transactions) rather than mining empty blocks.
However, this adds additional problems with senders needing to compete to have their transactions on the block. While there are measures in place such as maxFee or maxPriorityFee at the block level, the demand of the ethereum network has led to users offering higher tip amounts to outbid others.

# This Project
While there have been initiatives to reduce gas costs, monitoring gas fees and analysis of the historical transaction data is essential. The Ethereum-ETL pipeline helps you easily and efficiently ingest ethereum transactions and blocks data into Google Cloud Storage Data Lake and Google BigQuery Data Warehouse for further analysis. It also features custom transformations in DBT to further analyse and report the gas usage and prices.
<!-- 
Tools such as Etherscan and other Gas Estimators have been in market to guide users on average Gas prices.  -->

Read more about introduction to ethereum here - https://ethereum.org/en/developers/docs/intro-to-ethereum/
Using Spark

Create a GCP service account with below permissions


# Data Source
export PATH_TO_GOOGLE_CREDENTIALS='/home/singh/keys/'
export GOOGLE_PROJECT_ID='quantum-fusion-417707'
export BUCKET_NAME='ethereum_etl_datalake'


cp dev.env .env


dbt debug --profiles-dir ./dev --project-dir ./ethereum_transformation/


dbt build --profiles-dir ./dev --project-dir ./ethereum_transformation/

