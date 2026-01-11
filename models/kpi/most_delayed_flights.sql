{{ config(materialized='table') }}

SELECT
  FlightNumber,
  Route,
  Airline,
  Average_delay_mins
FROM (
  SELECT
    FlightNumber,
    OriginAirportCode || '-' || DestinationAirportCode AS Route,
    'Spice_Jet' AS Airline,
    ROUND(AVG(TIMESTAMP_DIFF(ActualDepartureDateTime, ScheduledDepartureDateTime, MINUTE)), 2) AS Average_delay_mins
  FROM {{ref("flight_details_sat_spicejet")}}
  GROUP BY FlightNumber, Route, Airline

  UNION ALL

  SELECT
    FlightNumber,
    OriginAirportCode || '-' || DestinationAirportCode AS Route,
    'Air_India' AS Airline,
    ROUND(AVG(TIMESTAMP_DIFF(ActualDepartureDateTime, ScheduledDepartureDateTime, MINUTE)), 2) AS Average_delay_mins
  FROM {{ref("flight_details_sat_airindia")}}
  GROUP BY FlightNumber, Route, Airline
)
