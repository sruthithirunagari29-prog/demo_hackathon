{{ config(materialized='table') }}

WITH passengers AS (
  SELECT passenger_details_hk, DOB FROM {{ ref("passenger_details_sat_airindia") }}
  UNION ALL
  SELECT passenger_details_hk, DOB FROM {{ ref("passenger_details_sat_spicejet") }}
),

bookings AS (
  SELECT booking_details_hk, CreatedDateTime FROM {{ ref("booking_details_sat_airindia") }}
  UNION ALL
  SELECT booking_details_hk, CreatedDateTime FROM {{ ref("booking_details_sat_spicejet") }}
)

SELECT
  FORMAT_DATE('%Y-%m', B.CreatedDateTime) AS month,
  CASE
    WHEN DATE_DIFF(DATE(B.CreatedDateTime), DATE(P.DOB), YEAR) BETWEEN 18 AND 25 THEN "18-25"
    WHEN DATE_DIFF(DATE(B.CreatedDateTime), DATE(P.DOB), YEAR) BETWEEN 26 AND 35 THEN "26-35"
    WHEN DATE_DIFF(DATE(B.CreatedDateTime), DATE(P.DOB), YEAR) BETWEEN 36 AND 50 THEN "36-50"
    WHEN DATE_DIFF(DATE(B.CreatedDateTime), DATE(P.DOB), YEAR) > 50 THEN "50+"
    ELSE "0-18"
  END AS age_group,
  COUNT(DISTINCT P.passenger_details_hk) AS volume,
  "frequent_flyers" AS kpi_type
FROM passengers P
JOIN {{ ref("bkg_flight_pass_det_link") }} L
  ON P.passenger_details_hk = L.passengerid_hk
JOIN bookings B
  ON L.bookingid_hk = B.booking_details_hk
GROUP BY month, age_group
