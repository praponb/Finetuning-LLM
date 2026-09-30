SELECT s.city, COUNT(*) AS product_count, COUNT(CASE WHEN p.category = 'Home' THEN 1 END) AS home_count FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id GROUP BY s.city;
