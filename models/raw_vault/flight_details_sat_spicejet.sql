{{ config(materialized='table') }}

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(FlightID AS STRING)))") }} AS flight_details_hk,
  {{ HACKATHON("COALESCE(CAST(FlightNumber AS STRING),'') || '|' || COALESCE(CAST(ArrivalDateTime AS STRING),'') || '|' || COALESCE(CAST(ScheduledDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(ActualDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(OriginAirportCode AS STRING),'') || '|' || COALESCE(CAST(DestinationAirportCode AS STRING),'') || '|' || COALESCE(CAST(SeatCapacity AS STRING),'') || '|' || COALESCE(CAST(AvailableSeats AS STRING),'')") }} AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'Spicejet_flight_details' AS main_source,
  FlightNumber, 
  ArrivalDateTime, 
  ScheduledDepartureDateTime, 
  ActualDepartureDateTime, 
  OriginAirportCode, 
  DestinationAirportCode, 
  SeatCapacity, 
  AvailableSeats
FROM {{ source('staging', 'Spicejet_flight_details') }}
