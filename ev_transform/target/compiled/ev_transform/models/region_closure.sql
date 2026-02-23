

WITH RECURSIVE rc AS (
    -- Base case: Every region is its own ancestor
    SELECT
        region_ons AS ancestor_ons,
        region_ons AS descendant_ons
    FROM "postgres"."public"."region_lookup"

    UNION ALL

    -- Recursive step: Find children of current descendants
    SELECT
        rc.ancestor_ons,
        rl.region_ons
    FROM rc
    JOIN "postgres"."public"."region_lookup" rl
        ON rl.parent_ons = rc.descendant_ons
)
SELECT DISTINCT * FROM rc