SELECT p.supplier_id, s.country, s.city, REGEXP_REPLACE(p.product_name, ' ', '') AS product_name_slug FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id;
