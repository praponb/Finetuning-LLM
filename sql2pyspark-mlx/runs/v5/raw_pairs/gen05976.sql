SELECT p.id, p.supplier_id, p.brand FROM products p WHERE p.launch_date BETWEEN '2023-04-01' AND '2025-04-01' AND p.supplier_id IN (SELECT id FROM suppliers WHERE country = 'UK')
