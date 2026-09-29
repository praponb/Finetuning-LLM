SELECT product_name, category FROM products WHERE brand IN ('Stark', 'Acme') EXCEPT SELECT product_name, category FROM products WHERE cost >= 620 ORDER BY product_name
