SELECT c.is_active, SUM(CASE WHEN o.status = 'SHIPPED' THEN o.discount ELSE 0 END) AS shipped_discount, MAX(o.amount) AS max_amount
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.is_active
GROUP BY c.is_active
ORDER BY max_amount DESC;
