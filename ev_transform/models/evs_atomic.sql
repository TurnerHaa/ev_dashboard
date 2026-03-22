{{ config(materialized='table') }}

WITH child_presence AS (
    -- Find all regions that are ANCESTORS of data that actually exists this quarter
    SELECT DISTINCT 
        rc.ancestor_ons, 
        e.quarter
    FROM {{ source('supabase_uploads', 'ev_registrations') }} e
    JOIN {{ ref('region_closure') }} rc 
        ON rc.descendant_ons = e.region_ons
    WHERE rc.ancestor_ons <> rc.descendant_ons
)

SELECT e.*
FROM {{ source('supabase_uploads', 'ev_registrations') }} e
LEFT JOIN child_presence cp 
    ON e.region_ons = cp.ancestor_ons 
    AND e.quarter = cp.quarter
-- If a region is in the "child_presence" list, it's a parent; we drop it.
WHERE cp.ancestor_ons IS NULL
