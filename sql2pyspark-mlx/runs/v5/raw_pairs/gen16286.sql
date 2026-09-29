SELECT p.category, p.brand, COUNT(*) AS rows_total, AVG(e.quantity) AS avg_quantity FROM inventory e INNER JOIN products p ON e.product_id = p.id GROUP BY ROLLUP (p.category, p.brand)
