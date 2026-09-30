SELECT products.id, products.cost
FROM products
WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.product_id = products.id AND inventory.quantity >= 179)
ORDER BY cost
