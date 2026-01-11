{{ config(materialized='table') }}
WITH src AS (
  SELECT 'Airindia_passenger_details' AS main_source, UPPER(TRIM(CAST(PassengerID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Airindia_passenger_details') }}
  UNION ALL
  SELECT 'Spicejet_passenger_details' AS main_source, UPPER(TRIM(CAST(PassengerID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Spicejet_passenger_details') }}
),
dedup AS (
  SELECT DISTINCT bkp_final, main_source FROM src
)
SELECT
 {{ HACKATHON("UPPER(TRIM(CAST(bkp_final AS STRING)))") }} AS passenger_details_hk,
  bkp_final AS passengerid,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
