SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, SUM(i.quantity) AS total_quantity
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
HAVING SUM(i.quantity) >= 1160
ORDER BY total_quantity DESC, opened_month ASC
LIMIT 50;
