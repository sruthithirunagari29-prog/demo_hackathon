{{ config(materialized='table') }}

WITH src AS (
  SELECT 
    'Airindia_booking_details' AS main_source, 
    UPPER(TRIM(CAST(BookingID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Airindia_booking_details') }}

  UNION ALL

  SELECT 
    'Spicejet_booking_details' AS main_source, 
    UPPER(TRIM(CAST(BookingID AS STRING))) AS bkp_final
  FROM {{ source('staging', 'Spicejet_booking_details') }}
),

dedup AS (
  SELECT DISTINCT 
    bkp_final, 
    main_source 
  FROM src
)

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(bkp_final AS STRING)))") }} AS booking_details_hk,
  bkp_final AS bookingid,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
