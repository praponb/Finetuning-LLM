SELECT p.id, p.stock_qty, p.cost, p.launch_date FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'DE');
