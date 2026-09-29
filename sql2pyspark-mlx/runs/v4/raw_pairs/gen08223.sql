WITH store_summary AS (
    SELECT store_id, SUM(revenue) AS total_revenue, MIN(revenue) AS min_revenue
    FROM store_sales
    WHERE revenue >= 2000
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    s.region,
    ss.total_revenue,
    ss.min_revenue
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
ORDER BY total_revenue DESC
