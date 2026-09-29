SELECT
    id,
    last_restock,
    product_id,
    quantity
FROM inventory
WHERE last_restock = (SELECT MAX(last_restock) FROM inventory WHERE quantity < 66)
ORDER BY product_id DESC, id
LIMIT 10;
