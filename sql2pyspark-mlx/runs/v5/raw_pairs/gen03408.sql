SELECT id, product_name FROM products WHERE brand <> 'Stark' UNION ALL SELECT id, product_name FROM products WHERE price > 460;
