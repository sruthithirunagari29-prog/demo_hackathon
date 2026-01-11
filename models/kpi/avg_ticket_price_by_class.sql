{{ config(materialized='table') }}

SELECT SeatClass,ROUND(AVG(AVG_TICKET)) AS AVG_TICKET_PRICE
FROM ((
  SELECT
    SeatClass,
    LBP.main_source,
    ROUND(AVG(SPAI.Amount)) AS AVG_TICKET
  FROM {{ref("bkg_payment_det_link") }} LBP
  JOIN {{ref("booking_details_sat_airindia")}} SB
    ON LBP.bookingid_hk = SB.booking_details_hk
  JOIN {{ref("payment_sat_airindia")}} SPAI
    ON LBP.paymentid_hk = SPAI.payment_hk
  GROUP BY SeatClass, main_source
)

UNION ALL

(SELECT
  SeatClass,
  LBP.main_source,
  ROUND(AVG(SPSJ.Amount)) AS AVG_TICKET
  FROM {{ref("bkg_payment_det_link")}} LBP
  JOIN {{ref("booking_details_sat_spicejet")}} SB
    ON LBP.bookingid_hk = SB.booking_details_hk
  JOIN {{ref("payment_sat_spicejet")}} SPSJ
    ON LBP.paymentid_hk = SPSJ.payment_hk
  GROUP BY SeatClass, main_source
))
GROUP BY SeatClass
