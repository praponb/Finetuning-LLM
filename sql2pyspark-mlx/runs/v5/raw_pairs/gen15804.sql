SELECT p.id, p.cost, p.stock_qty FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE supplier_name LIKE 'Lio%')
