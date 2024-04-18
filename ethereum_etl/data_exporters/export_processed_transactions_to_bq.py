from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    PROJECT = os.getenv('GOOGLE_PROJECT_ID')
    DATASET = 'ethereum_master'
    table_id = f"{PROJECT}.{DATASET}.ethereum_transactions"
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    df=df.toPandas()
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',
        overwrite_types=None,  # Specify resolution policy if table name already exists
    )
