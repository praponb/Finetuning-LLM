SELECT p.id, p.supplier_id, p.product_name FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE store_id IN (3, 4));
