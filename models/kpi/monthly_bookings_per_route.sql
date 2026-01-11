{{ config(materialized='table') }}

WITH airindia AS (
  SELECT
    FD.OriginAirportCode || '-' || FD.DestinationAirportCode AS route,
    FORMAT_DATE('%Y-%m', BD.CreatedDateTime) AS month,
    COUNT(*) AS total_bookings
  FROM {{ ref("booking_details_sat_airindia") }} BD
  JOIN {{ ref("bkg_flight_pass_det_link") }} LBFP
    ON LBFP.bookingid_hk = BD.booking_details_hk
   AND LBFP.main_source = 'Airindia_booking_details'
  JOIN {{ ref("flight_details_sat_airindia") }} FD
    ON LBFP.raw_flightid = FD.FlightNumber   -- ✅ use raw_flightid vs FlightNumber
  GROUP BY route, month
),

spicejet AS (
  SELECT
    FD.OriginAirportCode || '-' || FD.DestinationAirportCode AS route,
    FORMAT_DATE('%Y-%m', BD.CreatedDateTime) AS month,
    COUNT(*) AS total_bookings
  FROM {{ ref("booking_details_sat_spicejet") }} BD
  JOIN {{ ref("bkg_flight_pass_det_link") }} LBFP
    ON LBFP.bookingid_hk = BD.booking_details_hk
   AND LBFP.main_source = 'Spicejet_booking_details'
  JOIN {{ ref("flight_details_sat_spicejet") }} FD
    ON LBFP.raw_flightid = FD.FlightNumber   -- ✅ same fix here
  GROUP BY route, month
)

SELECT
  route,
  month,
  SUM(total_bookings) AS total_bookings,
  "monthly_bookings_per_route" AS kpi_type
FROM (
  SELECT * FROM airindia
  UNION ALL
  SELECT * FROM spicejet
)
GROUP BY route, month
