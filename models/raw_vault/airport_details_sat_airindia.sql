{{ config(materialized='table') }}

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(Airport_Code AS STRING)))") }} AS airport_details_hk,
  {{ HACKATHON("COALESCE(CAST(Airport_Name AS STRING),'')") }} AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'Airindia_airport_details' AS main_source,
  Airport_Name
FROM {{ source('staging', 'Airindia_airport_details') }}
