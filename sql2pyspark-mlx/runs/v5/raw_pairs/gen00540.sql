SELECT s.id, s.city, COUNT(p.id) AS product_count FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id GROUP BY s.id, s.city HAVING COUNT(p.id) > 4
