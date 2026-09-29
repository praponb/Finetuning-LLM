SELECT s.city, ROUND(AVG(o.shipping_fee), 2) AS avg_shipping_fee, SUM(CASE WHEN o.status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
RIGHT JOIN stores s ON o.store_id = s.id
WHERE s.opened_date < '2023-10-01'
GROUP BY s.city
HAVING SUM(CASE WHEN o.status = 'PENDING' THEN 1 ELSE 0 END) > 69
