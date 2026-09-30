SELECT s.id, s.rating, s.city FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.brand IN ('Stark', 'Umbrella', 'Initech')) ORDER BY 3, rating DESC
