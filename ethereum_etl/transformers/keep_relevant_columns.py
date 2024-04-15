from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.SELECT

    Docs: https://docs.mage.ai/guides/transformer-blocks#select-columns
    """
    transaction_columns = ['gas', 'nonce', 'value', 'block_number', 'gas_price',
       'receipt_cumulative_gas_used', 'receipt_gas_used', 'receipt_status',
       'receipt_effective_gas_price', 'transaction_type', 'max_fee_per_gas',
       'max_priority_fee_per_gas', 'block_timestamp', 'date', 'last_modified']
    
    action = build_transformer_action(
        df,
        action_type=ActionType.SELECT,
        arguments=transaction_columns,  # Specify columns to select
        axis=Axis.COLUMN,
    )

    return BaseAction(action).execute(df)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
