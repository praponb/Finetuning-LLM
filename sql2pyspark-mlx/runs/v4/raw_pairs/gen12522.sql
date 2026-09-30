SELECT p.id, p.stock_qty FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE units < 38) ORDER BY stock_qty DESC;
