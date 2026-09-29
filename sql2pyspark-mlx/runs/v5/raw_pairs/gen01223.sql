SELECT quantity, store_id, product_id
FROM inventory
WHERE quantity BETWEEN 89 AND 208
ORDER BY product_id DESC
