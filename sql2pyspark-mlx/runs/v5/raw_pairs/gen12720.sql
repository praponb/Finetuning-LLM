SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, COUNT(i.quantity) AS quantity_count, SUM(i.quantity) AS total_quantity
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
HAVING quantity_count >= 3
ORDER BY total_quantity
