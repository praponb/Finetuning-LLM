SELECT
    id,
    revenue,
    store_id,
    FIRST_VALUE(store_id) OVER (ORDER BY revenue, id) AS first_store_id
FROM store_sales
WHERE product_id = 10
