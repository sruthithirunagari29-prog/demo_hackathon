{{ config(materialized='table') }}

WITH unioned AS (

	  SELECT 
	    'Airindia_flight_details' AS main_source,
	    {{ HACKATHON("COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(OriginAirportCode AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(DestinationAirportCode AS STRING))) AS STRING),'')") }} AS airport_flight_det_lk_hk,
	    {{ HACKATHON("UPPER(TRIM(CAST(FlightID AS STRING)))") }} AS flightid_hk,
	    {{ HACKATHON("UPPER(TRIM(CAST(OriginAirportCode AS STRING)))") }} AS originairportcode_hk,
	    {{ HACKATHON("UPPER(TRIM(CAST(DestinationAirportCode AS STRING)))") }} AS destinationairportcode_hk
	  FROM {{ source('staging', 'Airindia_flight_details') }}

	  UNION ALL

	  SELECT 
	    'Spicejet_flight_details' AS main_source,
	    {{ HACKATHON("COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(OriginAirportCode AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(DestinationAirportCode AS STRING))) AS STRING),'')") }} AS airport_flight_det_lk_hk,
	    {{ HACKATHON("UPPER(TRIM(CAST(FlightID AS STRING)))") }} AS flightid_hk,
	    {{ HACKATHON("UPPER(TRIM(CAST(OriginAirportCode AS STRING)))") }} AS originairportcode_hk,
	    {{ HACKATHON("UPPER(TRIM(CAST(DestinationAirportCode AS STRING)))") }} AS destinationairportcode_hk
	  FROM {{ source('staging', 'Spicejet_flight_details') }}

),

dedup AS (
	  SELECT DISTINCT 
	    airport_flight_det_lk_hk, 
	    flightid_hk, 
	    originairportcode_hk, 
	    destinationairportcode_hk, 
	    main_source
	  FROM unioned
)

SELECT
  airport_flight_det_lk_hk,
  flightid_hk, 
  originairportcode_hk, 
  destinationairportcode_hk,
  CURRENT_TIMESTAMP() AS load_dts,
  main_source
FROM dedup
