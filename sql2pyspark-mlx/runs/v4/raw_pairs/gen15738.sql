SELECT s.city, COUNT(p.id) AS product_count, ROUND(AVG(p.stock_qty), 2) AS avg_stock_qty FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id GROUP BY s.city
