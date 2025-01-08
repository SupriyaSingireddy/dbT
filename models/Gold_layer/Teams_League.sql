{{ config(materialized='table') }}

WITH match_data AS (
    SELECT 
        DISTINCT match_name, team
    FROM Data.silver
),
Teams_League AS (
    SELECT 
        DISTINCT League_id, League_Name
    FROM   {{ ref('League_type') }}  -- Reference to the 'League_type' model
),
Team_Player AS (
    SELECT DISTINCT team_id, Team_Name
    FROM {{ ref('Team') }}
)

SELECT  
    B.League_id,
    C.team_id,
    UUID_STRING() AS Team_League_xref_id   
FROM 
    match_data A
INNER JOIN 
    Teams_League B    
    ON A.match_name = B.League_Name
INNER JOIN
    Team_Player C
    ON A.team = C.Team_Name  -- Fixed column name to match 'team'
