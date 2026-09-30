WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        LOWER(store_name) AS store_name_lower
    FROM stores
    WHERE opened_date > DATE '2023-07-05'
      AND region IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < s.opened_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        store_name_lower,
        NTILE(4) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2
    FROM eligible
)
-- Grouped metrics
SELECT
    city,
    AVG(sqft_bucket) AS avg_sqft_bucket,
    ROUND(AVG(sqft_avg_2), 2) AS avg_sqft_avg_2,
    STDDEV_SAMP(sqft) AS sqft_stddev
FROM with_windows
GROUP BY city
ORDER BY avg_sqft_avg_2 DESC NULLS LAST
