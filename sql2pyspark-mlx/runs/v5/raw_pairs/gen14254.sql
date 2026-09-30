WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(sqft, 0) AS sqft_filled,
        INITCAP(TRIM(store_name)) AS store_name_title
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date > TO_DATE('2023-05-09')
)
-- Keep one row per key (latest first)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    store_name_title
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS rn
    FROM filtered
) t
WHERE rn = 1
