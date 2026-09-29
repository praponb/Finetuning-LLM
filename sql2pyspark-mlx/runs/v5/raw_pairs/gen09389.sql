SELECT p.id, p.product_name, p.category, p.price FROM products p WHERE MONTH(p.launch_date) IN (2, 5, 11) AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city IN ('London', 'Bangkok'))
