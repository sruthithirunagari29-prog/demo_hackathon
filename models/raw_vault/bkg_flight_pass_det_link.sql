{{ config(materialized='table') }}

WITH unioned AS (

  SELECT 
    'Airindia_booking_details' AS main_source,
    {{ HACKATHON("COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PassengerID AS STRING))) AS STRING),'')") }} AS bkg_flight_pass_det_lk_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(BookingID AS STRING)))") }} AS bookingid_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(FlightID AS STRING)))") }} AS flightid_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(PassengerID AS STRING)))") }} AS passengerid_hk
  FROM {{ source('staging', 'Airindia_booking_details') }}

  UNION ALL

  SELECT 
    'Spicejet_booking_details' AS main_source,
    {{ HACKATHON("COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PassengerID AS STRING))) AS STRING),'')") }} AS bkg_flight_pass_det_lk_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(BookingID AS STRING)))") }} AS bookingid_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(FlightID AS STRING)))") }} AS flightid_hk,
    {{ HACKATHON("UPPER(TRIM(CAST(PassengerID AS STRING)))") }} AS passengerid_hk
  FROM {{ source('staging', 'Spicejet_booking_details') }}

),

dedup AS (
  SELECT DISTINCT 
    bkg_flight_pass_det_lk_hk, 
    bookingid_hk, 
    flightid_hk, 
    passengerid_hk, 
    main_source
  FROM unioned
)

SELECT
  bkg_flight_pass_det_lk_hk,
  bookingid_hk, 
  flightid_hk, 
  passengerid_hk,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
