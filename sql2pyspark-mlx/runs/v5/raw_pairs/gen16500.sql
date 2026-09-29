SELECT p.id, p.product_name FROM products p WHERE SIZE(p.tags) >= 1 AND p.supplier_id IN (SELECT id FROM suppliers WHERE supplier_name IS NOT NULL) ORDER BY product_name
