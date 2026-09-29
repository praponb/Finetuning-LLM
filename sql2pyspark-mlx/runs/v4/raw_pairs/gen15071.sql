SELECT ss.id, ss.units, ss.sale_date FROM store_sales ss WHERE ss.units BETWEEN 18 AND 39 AND ss.product_id NOT IN (SELECT id FROM products WHERE product_name LIKE 'USB%');
