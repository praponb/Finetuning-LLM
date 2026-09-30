SELECT
    product_id,
    store_id,
    MIN(last_restock) AS first_last_restock,
    AVG(quantity) AS avg_quantity
FROM inventory
WHERE quantity <> 146 AND quantity BETWEEN 81 AND 179 AND store_id IN (3, 4, 6)
GROUP BY product_id, store_id
HAVING AVG(quantity) >= 110
ORDER BY product_id DESC;
