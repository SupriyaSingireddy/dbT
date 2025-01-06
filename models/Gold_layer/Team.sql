{{ config(materialized='table') }}

WITH Team_data AS (
    SELECT 
        DISTINCT team AS Team_Name,
    FROM data.silver
    WHERE team IS NOT NULL  -- Optional: filter out NULL team values
)

SELECT UUID_STRING() AS team_id, Team_Name
FROM Team_data
