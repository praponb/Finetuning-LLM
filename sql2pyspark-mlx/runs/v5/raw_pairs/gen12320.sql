WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-04-29'
      AND units != 22
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(revenue, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY units, id) AS units_bucket,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS LAST) AS revenue_percent_rank
    FROM cleaned
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY units_bucket DESC NULLS LAST) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.sale_date,
        e.units,
        e.revenue,
        e.revenue_per_unit,
        e.units_bucket,
        p.cost,
        p.category,
        p.price
    FROM deduped e
    LEFT JOIN products p
        ON e.product_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN price >= 1090 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    COUNT(*) FILTER (WHERE category = 'Books') AS books_rows,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    ROUND(AVG(price), 2) AS avg_price
FROM with_dims
GROUP BY price_tier, week_start
ORDER BY books_rows NULLS FIRST
