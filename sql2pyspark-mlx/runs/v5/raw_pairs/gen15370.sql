SELECT p.id, p.launch_date, p.price, p.brand, s.rating FROM products p LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id;
