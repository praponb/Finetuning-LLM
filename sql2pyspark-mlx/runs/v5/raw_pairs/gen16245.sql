SELECT p.cost, p.supplier_id, s.rating, s.country, s.supplier_name, SUBSTRING(p.brand, 1, 3) AS brand_prefix FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE s.country = 'JP';
