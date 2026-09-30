SELECT
    product_id,
    store_id,
    COUNT(DISTINCT quantity) AS unique_products,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
GROUP BY product_id, store_id
