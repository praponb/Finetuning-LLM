WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE sale_date >= DATE '2023-12-31'
)
SELECT
    CASE WHEN revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    CASE WHEN days_since_sale_date >= 83 THEN 'HIGH' ELSE 'LOW' END AS days_since_sale_date_tier,
    PERCENTILE_APPROX(revenue, 0.95) AS revenue_p95,
    PERCENTILE_APPROX(revenue, 0.9) AS revenue_p90,
    AVG(revenue) AS avg_revenue,
    MIN(days_since_sale_date) AS min_days_since_sale_date,
    SUM(revenue) AS total_revenue,
    GROUPING_ID() AS gid
FROM cleaned
GROUP BY CUBE (CASE WHEN revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_sale_date >= 83 THEN 'HIGH' ELSE 'LOW' END);
