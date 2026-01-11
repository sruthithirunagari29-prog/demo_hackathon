{{ config(materialized='table') }}

WITH flight_hub AS (
    SELECT 
        flight_details_hk,
        flightid,
        main_source AS flight_source
    FROM {{ ref('flight_details_hub') }}
),

booking_payment_link AS (
    SELECT 
        bookingid_hk,
        paymentid_hk,
        flightid_hk
    FROM {{ ref('bkg_payment_det_link') }}
    JOIN {{ ref('bkg_flight_pass_det_link') }} USING (bookingid_hk)
),

payment_sat AS (
    SELECT 
        payment_hk,
        CAST(Amount AS FLOAT64) AS amount,
        PaymentMethod,
        Status
    FROM {{ ref('payment_sat_airindia') }}
    UNION ALL
    SELECT 
        payment_hk,
        CAST(Amount AS FLOAT64) AS amount,
        PaymentMethod,
        Status
    FROM {{ ref('payment_sat_spicejet') }}
),

revenue_per_flight AS (
    SELECT 
        f.flightid,
        f.flight_source,
        SUM(p.amount) AS total_revenue,
        COUNT(DISTINCT b.bookingid_hk) AS total_bookings
    FROM flight_hub f
    JOIN booking_payment_link b
      ON f.flight_details_hk = b.flightid_hk
    JOIN payment_sat p
      ON b.paymentid_hk = p.payment_hk
    WHERE p.Status = 'Complete'
    GROUP BY f.flightid, f.flight_source
)

SELECT
    flightid,
    flight_source,
    total_bookings,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM revenue_per_flight
ORDER BY total_revenue DESC
