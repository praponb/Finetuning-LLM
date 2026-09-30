SELECT p.id, p.product_name, p.brand, p.stock_qty FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country IN ('US', 'TH'));
