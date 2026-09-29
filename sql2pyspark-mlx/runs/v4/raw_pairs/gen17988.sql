SELECT p.id, p.launch_date, p.price FROM products p WHERE p.price >= 310 AND p.supplier_id IN (SELECT id FROM suppliers WHERE city NOT IN ('New York', 'Chiang Mai', 'Berlin'))
