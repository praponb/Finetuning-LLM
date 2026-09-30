SELECT quantity_bucket, quantity_count, max_quantity
FROM (
    SELECT CASE WHEN quantity >= 267 THEN 'high' WHEN quantity >= 41 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(quantity) AS quantity_count, MAX(quantity) AS max_quantity
    FROM inventory
    WHERE product_id IN (2, 8, 9)
    GROUP BY CASE WHEN quantity >= 267 THEN 'high' WHEN quantity >= 41 THEN 'mid' ELSE 'low' END
) AS sub
WHERE quantity_count < 8
ORDER BY max_quantity DESC, quantity_bucket
LIMIT 3
