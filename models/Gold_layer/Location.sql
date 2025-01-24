{{ config(materialized='table') }}

WITH Location_data AS (
    SELECT DISTINCT
         match_city,
         match_venue,
         match_id,
    FROM
        Data.Silver
)

SELECT 
    UUID_STRING() AS location_id, 
    match_city, 
    match_venue,
    match_id,
FROM Location_data
