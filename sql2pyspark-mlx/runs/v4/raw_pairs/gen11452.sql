WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN region = 'North' THEN 1 ELSE 0 END AS is_north,
        CASE WHEN city IN ('Bangkok', 'Chiang Mai') THEN 'Group A' ELSE 'Other' END AS city_group,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-04-25'
      AND region IS NOT NULL
)
-- QUALIFY-style window filter
SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date NULLS FIRST, id) AS dedup_rank
FROM filtered
QUALIFY dedup_rank = 1;
