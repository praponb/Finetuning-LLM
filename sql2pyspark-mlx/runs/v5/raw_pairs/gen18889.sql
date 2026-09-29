SELECT p.id, p.supplier_id FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE country NOT IN ('US', 'TH', 'SG')) ORDER BY 2, id LIMIT 1;
