SELECT id, last_restock, product_id
FROM inventory
WHERE last_restock = (SELECT MAX(last_restock) FROM inventory)
