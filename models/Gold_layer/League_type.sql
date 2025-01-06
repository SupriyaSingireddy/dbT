{{ config(materialized='table') }}

WITH League_data AS (
    SELECT 
        DISTINCT Match_Name As League_Name, Team_Type, Match_Type
    FROM Data.silver
    WHERE Match_Name IS NOT NULL  -- Optional: filter out NULL player values
)

SELECT UUID_STRING() AS League_id,Team_Type As League_Type, Match_Type, League_Name
FROM League_data
