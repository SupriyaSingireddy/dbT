{{ config(materialized='table') }}

WITH City_data AS (
    SELECT MATCH_ID, match_city, match_venue 
    FROM Data.cricket 
    GROUP BY MATCH_ID, match_city, match_venue
)
SELECT 
    CAST(t1.id AS INT) AS id,  -- Assuming id is an integer
    CAST(t1.batter AS VARCHAR(255)) AS batter,  -- Assuming batter is a string
    CAST(t1.bowler AS VARCHAR(255)) AS bowler,  -- Assuming bowler is a string
    CAST(t1.non_striker AS VARCHAR(255)) AS non_striker,  -- Assuming non_striker is a string
    CAST(t1.runs_batter AS INT) AS runs_batter,  -- Assuming runs_batter is an integer
    CAST(t1.runs_extras AS INT) AS runs_extras,  -- Assuming runs_extras is an integer
    CAST(t1.runs_total AS INT) AS runs_total,  -- Assuming runs_total is an integer
    CAST(t1.delivery_number AS INT) AS delivery_number,  -- Assuming delivery_number is an integer
    CAST(t1.over_numer AS INT) AS over_number,  -- Corrected column name from over_numer to over_number
    CAST(t1.team AS VARCHAR(255)) AS team,  -- Assuming team is a string
    CAST(t1.inning_number AS INT) AS inning_number,  -- Assuming inning_number is an integer
    CAST(t1.match_name AS VARCHAR(255)) AS match_name,  -- Assuming match_name is a string
    REPLACE(CAST(t1.MATCH_ID AS VARCHAR(255)), '.json', '') AS MATCH_ID,  -- MATCH_ID as string, removing ".json"
    CAST(t1.match_city AS VARCHAR(255)) AS match_city,  -- match_city as string
    CAST(t1.match_venue AS VARCHAR(255)) AS match_venue,  -- match_venue as string
    CAST(t1.match_type AS VARCHAR(255)) AS match_type,  -- match_type as string
    CAST(t1.team_type AS VARCHAR(255)) AS team_type,  -- team_type as string
    TO_DATE(t1.match_start_date, 'DD-MM-YYYY') AS match_start_date,  -- Convert to DATE if necessary (MySQL)
    CAST(COALESCE(t1.player_out, 'Not out') AS VARCHAR(255)) AS player_out  -- player_out as string, handling NULL values
FROM Data.cricket t1
JOIN City_data t2 
    ON t1.MATCH_ID = t2.MATCH_ID
    AND t1.match_venue = t2.match_venue  -- ensuring the join is correct on both MATCH_ID and match_venue
WHERE 
    -- Exclude rows where runs_total exceeds 8
    CAST(t1.runs_total AS INT) <= 8
    -- Exclude rows with NULL values in any column except player_out
    AND t1.id IS NOT NULL
    AND t1.batter IS NOT NULL
    AND t1.bowler IS NOT NULL
    AND t1.non_striker IS NOT NULL
    AND t1.runs_batter IS NOT NULL
    AND t1.runs_extras IS NOT NULL
    AND t1.runs_total IS NOT NULL
    AND t1.delivery_number IS NOT NULL
    AND t1.over_numer IS NOT NULL
    AND t1.team IS NOT NULL
    AND t1.inning_number IS NOT NULL
    AND t1.match_name IS NOT NULL
    AND t1.MATCH_ID IS NOT NULL
    AND t1.match_city IS NOT NULL
    AND t1.match_venue IS NOT NULL
    AND t1.match_type IS NOT NULL
    AND t1.team_type IS NOT NULL
    AND t1.match_start_date IS NOT NULL  -- Ensure that match_start_date is not NULL
    -- Exclude rows with invalid inning_number
    AND t1.inning_number IN (1, 2, 3, 4)  -- Only include valid inning numbers (1, 2, 3, or 4)
