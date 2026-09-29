SELECT p.cost, p.price, s.country, SUBSTRING(s.supplier_name, 1, 3) AS supplier_name_prefix FROM products p FULL OUTER JOIN suppliers s ON p.supplier_id = s.id;
