{{ config(materialized='table') }}

WITH player_data AS (
    SELECT 
        DISTINCT player_name
    FROM (
        SELECT batter AS player_name FROM Data.silver
        UNION ALL
        SELECT bowler AS player_name FROM Data.silver
        UNION ALL
        SELECT non_striker AS player_name FROM Data.silver
    ) AS all_players
    WHERE player_name IS NOT NULL  -- Optional: filter out NULL player values
)

SELECT CONCAT('P_', ROW_NUMBER() OVER (ORDER BY player_name)) AS player_id, player_name
FROM player_data
