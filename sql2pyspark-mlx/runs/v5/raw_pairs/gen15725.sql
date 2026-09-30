WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(store_name) AS store_name_lower
    FROM stores
    WHERE opened_date > TO_DATE('2023-10-01')
      AND sqft = 12632
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock <= s.opened_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS dedup_rank
    FROM eligible
    QUALIFY dedup_rank = 1
),
filtered_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(sqft, dedup_rank) AS least_sqft_dedup_rank,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM ranked
    WHERE region IS NOT DISTINCT FROM 'North'
      AND region IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY least_sqft_dedup_rank, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
        AVG(sqft) OVER (ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3
    FROM filtered_stores
),
remaining_keys AS (
    -- Set difference on keys
    SELECT region, id
    FROM with_windows
    EXCEPT
    SELECT region, id
    FROM stores
    WHERE city = 'Tokyo'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.region = k.region AND s.id = k.id
)
SELECT
    store_name,
    city,
    opened_date,
    sqft,
    store_name_lower,
    dedup_rank,
    least_sqft_dedup_rank,
    resolved_region,
    sqft_avg_5,
    CASE WHEN city RLIKE '^(New York|Tokyo)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS city_zone
FROM reconciled
ORDER BY city_zone NULLS FIRST
