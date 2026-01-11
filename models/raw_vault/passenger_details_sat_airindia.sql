{{ config(materialized='table') }}

SELECT
  {{ HACKATHON("UPPER(TRIM(CAST(PassengerID AS STRING)))") }} AS passenger_details_hk,
  {{ HACKATHON("COALESCE(CAST(FirstName AS STRING),'') || '|' || COALESCE(CAST(LastName AS STRING),'') || '|' || COALESCE(CAST(DOB AS STRING),'') || '|' || COALESCE(CAST(Email AS STRING),'') || '|' || COALESCE(CAST(CreatedDateTime AS STRING),'') || '|' || COALESCE(CAST(LastupdateDateTime AS STRING),'')") }} AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'Airindia_passenger_details' AS main_source,
  FirstName, 
  LastName, 
  DOB, 
  Email, 
  CreatedDateTime, 
  LastupdateDateTime
FROM {{ source('staging', 'Airindia_passenger_details') }}
