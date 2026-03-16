{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('supabase_uploads', 'chargers') }}
),
closure AS (
    SELECT * FROM {{ ref('region_closure') }}
)

SELECT
    c.region_ons,
    c.quarter,
    c.all_chargers,
    c.fast_chargers
FROM raw_data c
WHERE NOT EXISTS (
    SELECT 1
    FROM closure rc
    JOIN raw_data c_child
        ON c_child.region_ons = rc.descendant_ons
        AND c_child.quarter = c.quarter
    WHERE rc.ancestor_ons = c.region_ons
    AND rc.descendant_ons <> c.region_ons
)