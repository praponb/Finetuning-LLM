SELECT
    id,
    store_id,
    product_id,
    revenue,
    RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue DESC) AS rnk
FROM store_sales
WHERE sale_date > '2025-06-01'
ORDER BY id
