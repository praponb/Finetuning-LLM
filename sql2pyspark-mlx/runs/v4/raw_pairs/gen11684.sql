SELECT s.id, s.country, COUNT(p.id) AS product_count FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id GROUP BY s.id, s.country HAVING product_count >= 4
