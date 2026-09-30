SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, COUNT(i.quantity) AS quantity_count, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
GROUP BY DATE_TRUNC('MONTH', s.opened_date)
HAVING quantity_count >= 1
ORDER BY quantity_count DESC, opened_month
LIMIT 5;
