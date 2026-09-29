WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(store_name)) store_name_normalized,
        SUBSTRING(region, 1, 4) region_prefix,
        COALESCE(region, 'N/A') resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date >= s.opened_date
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        region_prefix,
        resolved_region,
        LOWER(store_name) store_name_lower
    FROM kept
    WHERE region != 'North'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.units <= 25
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        region_prefix,
        store_name_lower,
        CONCAT_WS('/', store_name, region) store_name_region_key,
        TRIM(region) clean_region
    FROM not_flagged
    WHERE region IS NOT NULL
)
SELECT
    id,
    city,
    region,
    region_prefix,
    store_name_lower,
    store_name_region_key,
    clean_region
FROM valid
ORDER BY id, city DESC
