SELECT o.id, o.amount, c.is_active
FROM orders o
LEFT OUTER JOIN customers c ON c.id = o.customer_id
WHERE c.segment = 'SMB'
ORDER BY o.id ASC, o.amount DESC
LIMIT 5;
