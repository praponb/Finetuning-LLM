SELECT
    id,
    product_id,
    quantity,
    RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rnk
FROM inventory
WHERE store_id IN (1, 2, 5, 6)
