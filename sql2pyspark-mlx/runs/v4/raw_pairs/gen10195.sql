WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS row_seq,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        NTILE(10) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket
    FROM stores
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.discount <= 0.34
        )
),
with_last AS (
    SELECT s.*, x.last_restock
    FROM kept s
    JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
totals AS (
    SELECT
        DATE_TRUNC('WEEK', last_restock) AS week_start,
        SIZE(COLLECT_SET(city)) AS num_cities,
        COUNT_IF(region = 'East') AS east_events,
        MIN(row_seq) AS min_row_seq,
        SUM(sqft_bucket) AS total_sqft_bucket,
        ROUND(SUM(sqft_bucket) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS sqft_bucket_per_west
    FROM with_last
    GROUP BY week_start
)
SELECT
    week_start,
    num_cities,
    east_events,
    total_sqft_bucket,
    sqft_bucket_per_west
FROM totals
ORDER BY east_events DESC NULLS LAST
