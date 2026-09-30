SELECT id, product_id, store_id
FROM inventory
WHERE quantity > 210 AND store_id IN (3, 4) AND MONTH(last_restock) IN (5, 6, 12)
ORDER BY store_id DESC
