SELECT p.id, p.stock_qty, p.brand, p.launch_date FROM products p WHERE p.id IN (SELECT product_id FROM inventory WHERE quantity > 75) ORDER BY stock_qty, brand;
