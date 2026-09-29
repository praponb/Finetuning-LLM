SELECT
    id,
    store_id,
    revenue,
    product_id,
    RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS rnk,
    NTILE(2) OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS bucket
FROM store_sales;
