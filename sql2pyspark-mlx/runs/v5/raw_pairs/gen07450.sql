SELECT s.id, COUNT(p.id) AS product_count FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id GROUP BY s.id ORDER BY s.id DESC LIMIT 50;
