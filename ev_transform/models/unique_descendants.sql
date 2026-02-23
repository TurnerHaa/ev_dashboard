{{ config(materialized='table') }}

SELECT DISTINCT descendant_ons 
FROM {{ ref('region_closure') }}