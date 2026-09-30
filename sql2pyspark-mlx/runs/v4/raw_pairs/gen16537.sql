SELECT
    id,
    store_id,
    revenue,
    SUM(revenue) OVER (PARTITION BY store_id) AS store_id_sum_revenue
FROM store_sales
