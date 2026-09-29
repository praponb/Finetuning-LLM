SELECT
    id,
    product_id,
    revenue,
    MIN(revenue) OVER (PARTITION BY product_id) AS product_id_min_revenue
FROM store_sales
