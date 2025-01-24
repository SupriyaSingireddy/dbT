{{ config(materialized='table') }}

WITH TeamsPerMatch AS (
    SELECT
        match_id,
        team AS team_name,
        ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY team) AS team_rank
    FROM
        data.silver
    GROUP BY
        match_id, team
),
BowlerTeamDetermination AS (
    SELECT
        c.batter,
        c.bowler,
        c.non_striker,
        c.match_id,
        c.inning_number AS Innings_Number,
        c.team AS batter_team_name,
        c.RUNS_BATTER,
        c.RUNS_EXTRAS,
        c.RUNS_TOTAL,
        c.DELIVERY_NUMBER,
        c.OVER_NUMBER,
        c.MATCH_START_DATE,
        c.PLAYER_OUT,
        c.MATCH_CITY,
        c.MATCH_VENUE,
        c.Team_Type, 
        c.Match_Type,
        c.match_Name,
        CASE
            WHEN t1.team_name = c.team THEN t2.team_name
            ELSE t1.team_name
        END AS Bowler_Team_Name
    FROM
        data.silver c
    JOIN
        TeamsPerMatch t1 ON c.match_id = t1.match_id AND t1.team_rank = 1
    JOIN
        TeamsPerMatch t2 ON c.match_id = t2.match_id AND t2.team_rank = 2
),
Player_idmodel AS (
    SELECT 
        player_id, 
        player_name
    FROM 
        {{ ref('player') }}  -- Reference the 'player' model
),
Team_idmodel AS (
    SELECT 
        team_id, 
        team_name
    FROM 
        {{ ref('Team') }}  -- Reference the 'team' model
),
location_idmodel AS (
    SELECT
        match_city,
        match_venue,
        location_id  -- This is the missing column alias
    FROM
        {{ ref('Location') }}  -- Reference the 'Location' model
),
League_idmodel AS (
    SELECT 
        League_Name, 
        League_Type, 
        Match_Type,
        League_id -- Assuming League_id is also part of this model
    FROM
        {{ ref('League_type') }}  -- Reference the 'League_type' model
)
SELECT 
    btd.match_id,
    -- Adding Player IDs from the 'Player_idmodel' (joined by player names)
    batter_player.player_id AS batter_id,
    bowler_player.player_id AS bowler_id,
    
    -- Adding Team IDs from the 'Team_idmodel' (joined by team names)
    batter_team.team_id AS batter_team_id,
    bowler_team.team_id AS bowler_team_id,
    
    btd.RUNS_BATTER,
    btd.RUNS_EXTRAS,
    btd.RUNS_TOTAL,
    btd.DELIVERY_NUMBER,
    btd.OVER_NUMBER,
    btd.MATCH_START_DATE,
    btd.PLAYER_OUT,
    btd.Innings_Number,
    FIRST_VALUE(CONCAT_WS(' Vs ', btd.batter_team_name, btd.Bowler_Team_Name)) 
        OVER (PARTITION BY btd.match_id ORDER BY btd.Innings_Number) AS Match_title,

    -- Adding Location ID from the 'location_idmodel' 
    location.location_id AS Location_id,

    -- Adding League ID from the 'League_idmodel' (using Team_Type and Match_Type)
    league.League_id AS League_id

FROM 
    BowlerTeamDetermination btd

-- Join for batter player ID from Player_idmodel (by batter player name)
LEFT JOIN 
    Player_idmodel batter_player ON btd.batter = batter_player.player_name

-- Join for bowler player ID from Player_idmodel (by bowler player name)
LEFT JOIN 
    Player_idmodel bowler_player ON btd.bowler = bowler_player.player_name

-- Join for batter team ID from Team_idmodel (by batter team name)
LEFT JOIN 
    Team_idmodel batter_team ON btd.batter_team_name = batter_team.team_name

-- Join for bowler team ID from Team_idmodel (by bowler team name)
LEFT JOIN 
    Team_idmodel bowler_team ON btd.Bowler_Team_Name = bowler_team.team_name

-- Join for location ID from location_idmodel (by match_city and match_venue)
LEFT JOIN 
    location_idmodel location ON btd.match_city = location.match_city AND btd.match_venue = location.match_venue

-- Join for League ID from League_idmodel (by Team_Type and Match_Type)
LEFT JOIN
    League_idmodel league ON btd.Team_Type = league.League_Type AND btd.Match_Type = league.Match_Type and btd.match_Name=League.League_Name
