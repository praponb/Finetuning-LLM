SELECT p.id, p.product_name FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE country = 'TH') ORDER BY product_name, p.id DESC
