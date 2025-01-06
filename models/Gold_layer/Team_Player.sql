{{ config(materialized='table') }}

WITH player_data AS (
    SELECT 
        DISTINCT player_name, team
    FROM (
        SELECT batter AS player_name, team FROM Data.silver
        UNION ALL
        SELECT bowler AS player_name, team FROM Data.silver
        UNION ALL
        SELECT non_striker AS player_name, team FROM Data.silver
    ) AS all_players
    WHERE player_name IS NOT NULL  -- Optional: filter out NULL player values
),
team_idmodel AS (
    SELECT 
        team_id, 
        Team_Name
    FROM 
        {{ ref('Team') }}  -- Reference the 'team' model
),
Player_idmodel AS (
    SELECT 
        Player_id, 
        Player_Name
    FROM 
        {{ ref('player') }}  -- Reference the 'team' model
)

SELECT  
    B.team_id,
    C.Player_id
FROM 
    player_data A
INNER JOIN 
    team_idmodel B    
ON 
    A.team = B.Team_Name
INNER JOIN
    Player_idmodel C
ON
    A.player_name = C.Player_Name

