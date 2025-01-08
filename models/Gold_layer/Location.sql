{{ config(materialized='table') }}

WITH Location_data AS (
    SELECT DISTINCT
         match_city,
         match_venue
    FROM
        Data.Silver
)

SELECT 
    UUID_STRING() AS location_id, 
    match_city, 
    match_venue
FROM Location_data
