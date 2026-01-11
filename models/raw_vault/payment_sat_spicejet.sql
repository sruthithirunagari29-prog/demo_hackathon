{{ config(materialized='table') }}

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(PaymentID AS STRING)))") }} AS payment_hk,
  {{ HACKATHON("COALESCE(CAST(PaymentMethod AS STRING),'') || '|' || COALESCE(CAST(Amount AS STRING),'') || '|' || COALESCE(CAST(TransactionDate AS STRING),'') || '|' || COALESCE(CAST(Status AS STRING),'')") }} AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'Spicejet_payment_details' AS main_source,
  PaymentMethod, Amount, TransactionDate, Status
FROM {{ source('staging', 'Spicejet_payment_details') }}
