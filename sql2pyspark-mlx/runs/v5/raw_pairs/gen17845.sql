SELECT id, product_id
FROM inventory
WHERE quantity > 212 AND store_id IN (3, 6);
