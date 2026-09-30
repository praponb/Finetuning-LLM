WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(sqft AS DOUBLE) AS sqft_str,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE YEAR(opened_date) = 2023
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (1, 3)
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(store_name, ' ', region) AS store_name_region,
        WEEKOFYEAR(opened_date) AS opened_date_weekofyear
    FROM kept
    WHERE opened_date > DATE '2023-06-15'
      AND region IS NOT NULL
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        region,
        sqft,
        sqft_str,
        region_filled,
        store_name_region,
        opened_date_weekofyear,
        SUBSTRING(store_name, 1, 3) AS store_name_prefix
    FROM cleaned
    WHERE opened_date > DATE '2023-09-11'
      AND region IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(opened_date_weekofyear) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS opened_date_weekofyear_avg_3,
        DENSE_RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_dense_rank
    FROM cleaned_stores
    WHERE sqft >= 10182
)
SELECT
    id,
    store_name_prefix,
    opened_date_weekofyear_avg_3,
    sqft_dense_rank
FROM with_windows
