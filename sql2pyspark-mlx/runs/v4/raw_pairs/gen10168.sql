SELECT p.id, p.launch_date FROM products p WHERE p.category = 'Electronics' AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city IN ('Bangkok', 'New York')) ORDER BY launch_date
