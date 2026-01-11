{{ config(materialized='table') }}

WITH src AS (
  SELECT 
    'Airindia_payment_details' AS main_source, 
    UPPER(TRIM(CAST(PaymentID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Airindia_payment_details') }}

  UNION ALL

  SELECT 
    'Spicejet_payment_details' AS main_source, 
    UPPER(TRIM(CAST(PaymentID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Spicejet_payment_details') }}
),

dedup AS (
  SELECT DISTINCT 
    bkp_final, 
    main_source 
  FROM src
)

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(bkp_final AS STRING)))") }} AS payment_hk,
  bkp_final AS paymentid,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
