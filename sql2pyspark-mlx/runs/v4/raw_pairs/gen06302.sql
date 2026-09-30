SELECT
    *,
    SUM(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
    COUNT(revenue) OVER (PARTITION BY store_id) AS store_id_count_revenue
FROM store_sales
