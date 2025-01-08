{{ config(materialized='table') }}

WITH bowler_aggregates AS (
    SELECT
        bowler,
        Team,
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
        SUM(runs_total) AS runs_given,
        -- Count the number of wickets per over for the bowler
        COUNT(CASE WHEN player_out != 'Not out' AND player_out IS NOT NULL THEN 1 ELSE NULL END) AS wickets
    FROM 
        data.silver
    GROUP BY 
        bowler, over_number, match_id, inning_number, Team
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
    B.match_id,
    T.team_id,
    PI.player_id AS Bowler_id,
    B.inning_number,
    B.over_number,
    B.fours_given,
    B.sixes_given,
    B.non_boundaries_given,
    B.extras_given,
    B.runs_given,  -- Total runs conceded by the bowler in that over
    B.number_of_deliveries,
    B.wickets,
    PI.player_name
FROM
    bowler_aggregates B 
INNER JOIN 
    Team_Player T 
    ON B.Team = T.team_name  -- Join match data with team details
INNER JOIN 
    Player_Info PI 
    ON B.bowler = PI.player_name  -- Join match data with player details (for bowler)
ORDER BY 
    B.match_id, B.inning_number, B.over_number, B.bowler
