{{ config(materialized='table') }}

WITH unioned AS (

  SELECT 
    'Airindia_booking_details' AS main_source,
    {{ HACKATHON("COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PaymentID AS STRING))) AS STRING),'')") }} AS bkg_payment_det_lk_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(BookingID AS STRING)))") }} AS bookingid_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(PaymentID AS STRING)))") }} AS paymentid_hk
  FROM {{ source('staging', 'Airindia_booking_details') }}

  UNION ALL

  SELECT 
    'Spicejet_booking_details' AS main_source,
    {{ HACKATHON("COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PaymentID AS STRING))) AS STRING),'')") }} AS bkg_payment_det_lk_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(BookingID AS STRING)))") }} AS bookingid_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(PaymentID AS STRING)))") }} AS paymentid_hk
  FROM {{ source('staging', 'Spicejet_booking_details') }}

),

dedup AS (
  SELECT DISTINCT 
    bkg_payment_det_lk_hk, 
    bookingid_hk, 
    paymentid_hk, 
    main_source
  FROM unioned
)

SELECT
  bkg_payment_det_lk_hk,
  bookingid_hk, 
  paymentid_hk,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
