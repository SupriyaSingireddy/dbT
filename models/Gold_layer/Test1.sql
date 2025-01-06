{{ config(materialized='table') }}

SELECT *
FROM {{ ref('Team') }} 
    