SELECT c.city, SUM(CASE WHEN o.status = 'PENDING' THEN o.amount ELSE 0 END) AS pending_amount
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
GROUP BY c.city
HAVING SUM(CASE WHEN o.status = 'PENDING' THEN o.amount ELSE 0 END) > 51;
