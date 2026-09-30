SELECT p.brand, COUNT(*) AS cnt, ROUND(AVG(a.quantity), 2) AS avg_quantity FROM order_items a INNER JOIN products p ON a.product_id = p.id GROUP BY p.brand ORDER BY brand DESC;
