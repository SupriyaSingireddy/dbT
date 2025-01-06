{{ config(materialized='table') }}

WITH match_data AS (
    SELECT 
        batter, 
        bowler, 
        non_striker, 
        runs_batter, 
        runs_extras, 
        runs_total, 
        delivery_number, 
        over_number, 
        team, 
        inning_number, 
        match_name, 
        match_id,
        match_city, 
        match_venue, 
        match_type, 
        team_type, 
        match_start_date,
        player_out
    FROM 
        Data.silver
),
bowler_aggregates AS (
    SELECT
        bowler,
        over_number,
        match_id,
        inning_number,
        -- Count how many balls in that over resulted in 4 runs
        COUNT(CASE WHEN runs_total = 4 THEN 1 ELSE NULL END) AS fours_given,
        -- Count how many balls in that over resulted in 6 runs
        COUNT(CASE WHEN runs_total = 6 THEN 1 ELSE NULL END) AS sixes_given,
        -- Count how many balls in that over did not result in 4 or 6 runs (non-boundaries)
        COUNT(CASE WHEN runs_total NOT IN (4, 6) AND runs_total > 0 THEN 1 ELSE NULL END) AS non_boundaries_given,
        -- Sum all extras given (wides, no-balls, etc.) in that over
        SUM(runs_extras) AS extras_given,
        -- Count total number of deliveries bowled in that over
        COUNT(*) AS number_of_deliveries,
        -- Sum up total runs given in that over, including 4s, 6s, and extras
        SUM(runs_total) AS runs_given
    FROM 
        match_data
    GROUP BY 
        bowler, over_number, match_id, inning_number
),
Team_Player AS (
    SELECT 
        team_id,
        team_name
    FROM 
        {{ ref('Team') }}  -- Reference to the 'Team' model
),
Player_Info AS (
    SELECT 
        player_id,
        player_name
    FROM 
        {{ ref('player') }}  -- Reference to the 'players' model
)

SELECT
    A.match_id,
    A.over_number,
    A.bowler AS bowler_id,  -- Bowler ID
    B.fours_given,
    B.sixes_given,
    B.non_boundaries_given,
    B.extras_given,
    B.runs_given,  -- Total runs conceded by the bowler in that over
    A.inning_number,
    B.number_of_deliveries,
    PI.player_id,
    PI.player_name
FROM
    match_data A
INNER JOIN 
    bowler_aggregates B 
    ON A.bowler = B.bowler 
    AND A.over_number = B.over_number 
    AND A.match_id = B.match_id  -- Aggregating by bowler and over number
INNER JOIN 
    Team_Player T 
    ON A.team = T.team_name  -- Join match data with team details
INNER JOIN 
    Player_Info PI 
    ON A.bowler = PI.player_name  -- Join match data with player details (for bowler)
ORDER BY 
    A.match_id, A.inning_number, A.over_number, A.bowler

