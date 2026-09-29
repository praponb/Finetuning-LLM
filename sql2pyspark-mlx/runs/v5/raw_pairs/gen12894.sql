WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-03-25')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_part AS units_bucket,
        FIRST_VALUE(units, TRUE) OVER (PARTITION BY store_id ORDER BY sale_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units
    FROM cleaned
    WINDOW w_part AS (PARTITION BY store_id ORDER BY units, id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(revenue) AS revenue_whole,
        ROUND(first_known_units / units_bucket, 2) AS first_known_units_per_units_bucket
    FROM with_windows
    WHERE sale_date > DATE '2023-09-23'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.sale_date,
        e.revenue,
        e.sale_date_month_end,
        e.units_bucket,
        e.revenue_whole,
        s.city,
        s.store_name
    FROM valid e
    LEFT JOIN stores s
        ON e.store_id = s.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', sale_date_month_end) AS activity_month,
    city,
    CASE WHEN revenue_whole >= 71 THEN 'HIGH' ELSE 'LOW' END AS revenue_whole_tier,
    PERCENTILE_APPROX(revenue, 0.5) AS revenue_p50,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
    APPROX_PERCENTILE(revenue_whole, ARRAY(0.25, 0.5, 0.75)) AS revenue_whole_quartiles,
    PERCENTILE_APPROX(revenue_whole, ARRAY(0.25, 0.5, 0.75)) AS revenue_whole_quartiles_2,
    PERCENTILE_APPROX(units_bucket, ARRAY(0.25, 0.5, 0.75)) AS units_bucket_quartiles,
    COUNT(*) AS row_count,
    GROUPING_ID() AS grouping_id
FROM enriched
GROUP BY CUBE (DATE_TRUNC('MONTH', sale_date_month_end), city, CASE WHEN revenue_whole >= 71 THEN 'HIGH' ELSE 'LOW' END);
