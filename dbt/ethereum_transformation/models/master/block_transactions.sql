{{ config(materialized='table') }}

WITH
  BLOCKS_DATA AS (
  SELECT
    block_number,
    block_size,
    block_base_fee_per_gas,
    block_gas_limit,
    block_gas_used,
    block_transaction_count,
    block_date
  FROM
    {{ source('ethereum_master', 'ethereum_blocks') }}
    ),
  TRANSACTIONS_DATA AS (
  SELECT
    gas,
    value,
    gas_price,
    max_fee_per_gas,
    last_modified,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    transaction_type,
    block_number,
    value_gwei,
    max_fee_per_gas_gwei,
    max_priority_fee_per_gas_gwei,
    receipt_effective_gas_price_gwei,
    gas_price_gwei,
  FROM
    {{ source('ethereum_master', 'ethereum_transactions') }}
    )

    SELECT * FROM TRANSACTIONS_DATA LEFT JOIN BLOCKS_DATA USING(block_number)
