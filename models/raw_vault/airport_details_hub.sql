{{ config(materialized='table') }}

WITH src AS (
  SELECT 
    'Airindia_airport_details' AS main_source, 
    UPPER(TRIM(CAST(Airport_Code AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Airindia_airport_details') }}

  UNION ALL

  SELECT 
    'Spicejet_airport_details' AS main_source, 
    UPPER(TRIM(CAST(Airport_Code AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Spicejet_airport_details') }}
),

dedup AS (
  SELECT DISTINCT 
    bkp_final, 
    main_source 
  FROM src
)

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(bkp_final AS STRING)))") }} AS airport_details_hk,
  bkp_final AS airport_code,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
