SELECT p.id, p.launch_date, p.category
FROM products p
WHERE p.product_name IS NOT NULL AND EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.last_restock > '2024-04-01');
