WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(city) AS city_lower
    FROM stores
    WHERE opened_date > DATE '2023-11-04'
      AND region IS NOT NULL
),
base_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        region,
        INITCAP(TRIM(city)) AS city_title
    FROM base
    WHERE opened_date >= DATE '2024-01-24'
      AND region IS NOT NULL
      AND UPPER(TRIM(store_name)) = 'SIAM FLAGSHIP'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base_stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id = 4
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.revenue IS NOT NULL
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity <= 205
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(store_name) AS store_name_length,
        COALESCE(region, 'N/A') AS region_clean
    FROM eligible
    WHERE LOWER(TRIM(store_name)) = 'riverside'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT
    SELECT id
    FROM stores
    WHERE opened_date <= '2024-04-21'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    city_title,
    region_clean
FROM reconciled
