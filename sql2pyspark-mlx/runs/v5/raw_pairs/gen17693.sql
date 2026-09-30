SELECT
    CASE WHEN quantity >= 262 THEN 'high' WHEN quantity >= 70 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    product_id,
    MAX(quantity) AS max_quantity,
    SUM(quantity) AS total_quantity
FROM inventory
WHERE quantity <= 132 AND product_id = 1 AND product_id IN (1, 4, 5, 9)
GROUP BY quantity_bucket, product_id
