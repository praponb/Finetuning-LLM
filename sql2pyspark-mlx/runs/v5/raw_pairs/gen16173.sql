SELECT p.product_name, p.category, s.city FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id;
