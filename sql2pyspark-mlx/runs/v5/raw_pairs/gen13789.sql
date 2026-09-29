WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(region, 'unassigned') AS region_filled,
        SUBSTRING(store_name, 1, 4) AS store_name_prefix,
        COALESCE(region, 'NONE') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date > DATE '2023-06-05'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(region) AS region_upper,
        COALESCE(region, 'NONE') AS region_filled_2
    FROM cleaned
    WHERE opened_date >= DATE '2023-07-19'
      AND region IS NOT NULL
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.quantity,
        x.last_restock
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        region_filled,
        store_name_prefix,
        resolved_region,
        region_upper,
        region_filled_2,
        LAG(sqft) OVER w_part AS prev_sqft,
        LEAD(sqft) OVER w_part AS next_sqft,
        LAG(quantity) OVER w_part AS prev_quantity,
        LEAD(quantity) OVER w_part AS next_quantity,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM with_latest
    WHERE sqft <> 8919
    WINDOW w_part AS (PARTITION BY region ORDER BY sqft, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT region
    FROM kept
    EXCEPT ALL
    SELECT region
    FROM stores
    WHERE sqft > 12201
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN kept_keys k ON s.region = k.region
)
-- Grouped metrics
SELECT
    city,
    CASE WHEN prev_sqft >= 16507 THEN 'LARGE' ELSE 'SMALL' END AS prev_sqft_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    PERCENTILE_APPROX(sqft, 0.95) AS sqft_p95,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles_2,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count
FROM reconciled
GROUP BY city, CASE WHEN prev_sqft >= 16507 THEN 'LARGE' ELSE 'SMALL' END
