WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        DATE_TRUNC('WEEK', opened_date) AS opened_date_period,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
      AND city IN ('Bangkok', 'Chiang Mai')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date <= s.opened_date_period
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY sqft DESC, id) AS sqft_bucket,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) AS event_seq,
        SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM kept
    WHERE city = 'London'
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_revenue
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM latest_match
) src
PIVOT (
    SUM(sqft) AS total,
    COUNT(1) AS item_count
    FOR region IN ('East', 'South')
);
