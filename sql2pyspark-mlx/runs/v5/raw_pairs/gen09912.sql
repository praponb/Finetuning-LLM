SELECT product_id, MAX(last_restock) last_last_restock, MAX(quantity) max_quantity
FROM inventory
WHERE product_id IN (7, 8)
GROUP BY product_id
HAVING MAX(quantity) > 210;
