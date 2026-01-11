{{ config(materialized='table') }}

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(BookingID AS STRING)))") }} AS booking_details_hk,
  {{ HACKATHON("COALESCE(CAST(FlightID AS STRING),'') || '|' || COALESCE(CAST(PassengerID AS STRING),'') || '|' || COALESCE(CAST(Status AS STRING),'') || '|' || COALESCE(CAST(BookingDate AS STRING),'') || '|' || COALESCE(CAST(SeatNumber AS STRING),'') || '|' || COALESCE(CAST(SeatClass AS STRING),'') || '|' || COALESCE(CAST(PaymentID AS STRING),'') || '|' || COALESCE(CAST(CreatedDateTime AS STRING),'') || '|' || COALESCE(CAST(LastupdateDateTime AS STRING),'')") }} AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'Airindia_booking_details' AS main_source,
  FlightID, 
  PassengerID, 
  Status, 
  BookingDate, 
  SeatNumber, 
  SeatClass, 
  PaymentID, 
  CreatedDateTime, 
  LastupdateDateTime
FROM {{ source('staging', 'Airindia_booking_details') }}
