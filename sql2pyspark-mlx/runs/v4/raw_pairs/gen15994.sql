SELECT p.id, p.product_name, p.price, p.cost FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE country IN ('UK', 'JP'));
