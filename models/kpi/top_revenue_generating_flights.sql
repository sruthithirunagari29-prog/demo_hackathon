{{ config(materialized='table') }}

WITH payments AS (
  SELECT payment_hk, Amount, TransactionDate FROM {{ ref("payment_sat_airindia") }}
  UNION ALL
  SELECT payment_hk, Amount, TransactionDate FROM {{ ref("payment_sat_spicejet") }}
),

flight_attribution AS (
  SELECT FlightNumber, 'AirIndia' AS airline FROM {{ ref("flight_details_sat_airindia") }}
  UNION ALL
  SELECT FlightNumber, 'SpiceJet' AS airline FROM {{ ref("flight_details_sat_spicejet") }}
),

joined AS (
  SELECT
    FORMAT_DATE('%Y-%m', p.TransactionDate) AS month,
    LBFPD.raw_flightid,
    FA.airline,
    SUM(p.Amount) AS value,
    COUNT(DISTINCT p.payment_hk) AS volume
  FROM payments p
  JOIN {{ ref("bkg_payment_det_link") }} LBPD
    ON p.payment_hk = LBPD.paymentid_hk
  JOIN {{ ref("bkg_flight_pass_det_link") }} LBFPD
    ON LBPD.bookingid_hk = LBFPD.bookingid_hk
  JOIN flight_attribution FA
    ON LBFPD.raw_flightid = FA.FlightNumber
  GROUP BY month, LBFPD.raw_flightid, FA.airline
)

SELECT
  month,
  raw_flightid,
  value,
  volume,
  airline,
  "monthly_revenue" AS kpi_type
FROM joined
QUALIFY ROW_NUMBER() OVER (PARTITION BY month, airline ORDER BY value DESC) = 1
