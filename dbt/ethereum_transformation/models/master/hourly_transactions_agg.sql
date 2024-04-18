{{ config(materialized='table',
            alias='hourly_transactions_agg' ) }}

SELECT
block_date,
EXTRACT(HOUR from block_timestamp) as HOUR_OF_DAY,
  -- block_number,
  AVG(gas_price_gwei) AS AVERAGE_GAS_PRICE,
  AVG(receipt_cumulative_gas_used) AS AVERAGE_receipt_cumulative_gas_used,
  AVG(receipt_gas_used) AS AVERAGE_RECEIPT_GAS_USED,
  AVG(gas) AS AVERAGE_GAS,
  AVG(gas - receipt_gas_used) as EXTRA_GAS
FROM
  {{ source('ethereum_master', 'ethereum_transactions') }}
WHERE
  receipt_gas_used=21000 -- normal transfer transactions
GROUP BY
block_date,
HOUR_OF_DAY
ORDER BY
block_date,
HOUR_OF_DAY