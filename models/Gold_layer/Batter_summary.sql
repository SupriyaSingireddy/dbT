{{ config(materialized='table') }}

WITH innings_data AS (
    SELECT
        batter, match_type, match_id,
        inning_number, Team,
        SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
        SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes,
        SUM(CASE WHEN runs_batter NOT IN (4, 6) THEN 1 ELSE 0 END) AS non_boundaries,
        SUM(runs_total) AS total_runs,
        COUNT(CASE WHEN runs_batter > 0 OR runs_extras > 0 THEN 1 END) AS balls_faced,
        SUM(runs_batter) AS score
    FROM
        Data.Silver
    GROUP BY
        batter, inning_number, match_type, match_id, Team
),
Player_idmodel AS (
    SELECT 
        Player_id, 
        Player_Name
    FROM 
        {{ ref('player') }}  -- Reference the 'player' model
),
Team_idmodel AS (  -- Fixed the alias here (removed space)
    SELECT 
        Team_id, 
        Team_Name
    FROM 
        {{ ref('Team') }}  -- Reference the 'Team' model
)

SELECT
    A.batter,
    B.Player_id,
    C.Team_id,
    A.inning_number,
    A.fours,
    A.sixes,
    A.non_boundaries,
    A.total_runs,
    A.balls_faced,
    A.score,
    A.match_type, 
    A.match_id
FROM
    innings_data AS A
INNER JOIN Player_idmodel AS B 
    ON A.batter = B.Player_Name
INNER JOIN Team_idmodel AS C  -- Join alias corrected
    ON A.Team = C.Team_Name
ORDER BY
    A.batter, A.inning_number

