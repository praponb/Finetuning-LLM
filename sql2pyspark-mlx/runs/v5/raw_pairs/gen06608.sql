WITH kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM stores
    EXCEPT DISTINCT
    SELECT id
    FROM stores
    WHERE opened_date <= '2023-11-13'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(store_name)) store_name_title
    FROM verified
    WHERE opened_date > DATE '2023-08-27'
      AND sqft != 14896
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= s.opened_date
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(sqft * 100, 2) sqft_adjusted,
        COALESCE(region, 'NONE') region_filled
    FROM not_flagged
    WHERE region IS NOT NULL
      AND region IS NOT DISTINCT FROM 'East'
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, region
    FROM valid
    EXCEPT DISTINCT
    SELECT city, region
    FROM stores
    WHERE region = 'East'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.city = k.city AND s.region = k.region
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    store_name_title,
    region_filled,
    CASE WHEN region RLIKE '^(East|South)$' THEN 'GROUP_A' ELSE 'CORE' END AS region_zone
FROM kept_rows
ORDER BY sqft DESC NULLS FIRST
