SELECT
    store_id,
    MAX(revenue) max_revenue,
    AVG(revenue) avg_revenue,
    SUM(revenue) total_revenue
FROM store_sales
GROUP BY store_id
