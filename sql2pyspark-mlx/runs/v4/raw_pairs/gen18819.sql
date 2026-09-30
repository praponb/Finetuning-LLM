SELECT id, quantity, product_id
FROM inventory
WHERE quantity < (SELECT MIN(quantity) FROM inventory);
