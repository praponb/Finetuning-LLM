WITH with_dims AS (
    SELECT
        a.*,
        b.city,
        b.sqft,
        b.opened_date
    FROM store_sales a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
enriched AS (
    SELECT e.*, b.launch_date
    FROM with_dims e
    LEFT JOIN products b
        ON e.product_id = b.id
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        city,
        sqft,
        opened_date,
        launch_date,
        LEAD(sqft, 2) OVER (PARTITION BY product_id, store_id ORDER BY revenue, id) AS next_sqft,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY revenue, id) AS revenue_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY units, id) AS seq_no
    FROM enriched
),
totals AS (
    SELECT
        CASE WHEN revenue >= 3700 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
        SIZE(COLLECT_SET(city)) AS num_cities,
        SUM(CASE WHEN city = 'Tokyo' THEN next_sqft ELSE 0 END) AS tokyo_next_sqft,
        COUNT(DISTINCT city) AS distinct_cities,
        SUM(CASE WHEN city = 'Singapore' THEN sqft ELSE 0 END) AS singapore_sqft,
        ROUND(SUM(revenue_bucket) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS revenue_bucket_per_new_york,
        PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles
    FROM metrics
    GROUP BY revenue_tier
)
SELECT
    revenue_tier,
    tokyo_next_sqft,
    distinct_cities,
    revenue_bucket_per_new_york
FROM totals
ORDER BY distinct_cities DESC;
