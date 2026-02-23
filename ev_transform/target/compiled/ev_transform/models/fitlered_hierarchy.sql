

WITH RECURSIVE closure AS (
    SELECT * FROM "postgres"."public"."region_lookup"
),

hierarchy_cte AS ( 
    SELECT 
        region_ons,
        region_name,
        parent,
        1 AS depth,
        ARRAY[region_name]::TEXT[] AS path_names
    FROM closure
    WHERE parent IS NULL OR region_name = 'United Kingdom'
    
    UNION ALL
    
    SELECT 
        c.region_ons,
        c.region_name,
        c.parent,
        p.depth + 1,
        p.path_names || c.region_name
    FROM closure c
    INNER JOIN hierarchy_cte p ON c.parent = p.region_name
    WHERE c.region_name <> p.region_name
    AND p.depth < 6
)

SELECT 
    region_ons,
    region_name,
    parent,
    depth,
    path_names[1] AS level1,
    path_names[2] AS level2,
    path_names[3] AS level3,
    path_names[4] AS level4,
    path_names[5] AS level5,
    path_names[6] AS level6
FROM hierarchy_cte