{{ config(materialized='table') }}

WITH src AS (
  SELECT 
    'Airindia_flight_details' AS main_source, 
    UPPER(TRIM(CAST(FlightID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Airindia_flight_details') }}

  UNION ALL

  SELECT 
    'Spicejet_flight_details' AS main_source, 
    UPPER(TRIM(CAST(FlightID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Spicejet_flight_details') }}
),

dedup AS (
  SELECT DISTINCT 
    bkp_final, 
    main_source 
  FROM src
)

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(bkp_final AS STRING)))") }} AS flight_details_hk,
  bkp_final AS flightid,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
