{{ config(materialized='table') }}

WITH innings_data AS (
    SELECT
        match_id,
        inning_number,
        match_city,
        match_venue,
        -- Categorize innings
        CASE 
            WHEN inning_number = 1 THEN 'first_inning'
            WHEN inning_number = 2 THEN 'second_inning'
            WHEN inning_number = 3 THEN 'third_inning'
            WHEN inning_number = 4 THEN 'fourth_inning'
            ELSE 'unknown_inning'
        END AS inning_category
    FROM
        Data.Silver  -- Reference to the Silver table for innings data
),

match_summary AS (
    -- Getting match details from the batter_summary table
    SELECT 
        match_id,
        team_id  -- Get the team_id from batter_summary table
    FROM 
        {{ ref('Batter_summary') }}  -- Reference the batter_summary table
),

teams_league AS (
    -- Getting league details from the teams_league_ref table
    SELECT 
        team_id,
        league_id
    FROM 
        {{ ref('Teams_League') }}  -- Reference the teams_league_ref table
),

location_details AS (
    -- Getting location id from the location table based on match_city and match_venue
    SELECT 
        match_city, 
        match_venue,
        location_id
    FROM 
        {{ ref('Location') }}  -- Reference the location table
),

-- Join the data to get match-specific details
final_summary AS (
    SELECT
        i.match_id,
        i.inning_category,
        m.team_id,
        t.league_id,
        l.location_id
    FROM 
        innings_data i
    JOIN 
        match_summary m ON i.match_id = m.match_id
    JOIN 
        teams_league t ON m.team_id = t.team_id
    JOIN 
        location_details l ON i.match_city = l.match_city AND i.match_venue = l.match_venue
)

-- Final SELECT to format the results with one row per match
SELECT 
    match_id,
    MAX(CASE WHEN inning_category = 'first_inning' THEN team_id END) AS First_Innings_Team_ID,
    MAX(CASE WHEN inning_category = 'second_inning' THEN team_id END) AS Second_Innings_Team_ID,
    MAX(CASE WHEN inning_category = 'third_inning' THEN team_id END) AS Third_Innings_Team_ID,
    MAX(CASE WHEN inning_category = 'fourth_inning' THEN team_id END) AS Fourth_Innings_Team_ID,
    MAX(league_id) AS League_ID,
    MAX(location_id) AS Location_ID
FROM 
    final_summary
GROUP BY
    match_id
