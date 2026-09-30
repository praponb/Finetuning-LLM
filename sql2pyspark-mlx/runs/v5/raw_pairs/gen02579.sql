WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(revenue) revenue_whole
    FROM store_sales
    WHERE revenue >= 1800
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) = 1
),
joined AS (
    -- Attach stores attributes
    SELECT src.*, p.opened_date
    FROM latest src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        p.opened_date AS store_opened_date,
        p.city,
        p.region
    FROM joined src
    LEFT JOIN stores p
        ON src.store_id = p.id
    WHERE p.region NOT IN ('West', 'North')
)
-- Aggregate with conditional metrics
SELECT
    region,
    CASE WHEN revenue >= 3000 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    SUM(CASE WHEN city = 'London' THEN revenue_whole ELSE 0 END) AS london_revenue_whole,
    CORR(units, revenue) AS corr_units_revenue,
    PERCENTILE_APPROX(revenue_whole, 0.95) AS revenue_whole_p95,
    ROUND(AVG(units), 2) AS avg_units
FROM with_dims
GROUP BY region, revenue_tier
ORDER BY revenue_tier
