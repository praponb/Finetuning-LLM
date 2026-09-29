SELECT s.region, DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, MIN(o.amount) AS min_amount
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE s.opened_date >= '2023-08-29'
GROUP BY s.region, DATE_FORMAT(s.opened_date, 'yyyy-MM');
