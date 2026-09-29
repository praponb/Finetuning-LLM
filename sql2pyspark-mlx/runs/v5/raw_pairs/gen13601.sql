WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-08-12'
      AND region IS DISTINCT FROM 'North'
)
-- Keep one row per key (latest first)
SELECT
    id,
    opened_date,
    store_name_normalized
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date NULLS LAST, id) AS dedup_rank
    FROM valid
) ranked
WHERE dedup_rank = 1;
