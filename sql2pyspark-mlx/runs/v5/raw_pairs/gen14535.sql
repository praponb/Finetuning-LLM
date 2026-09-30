SELECT c.is_active, a.customer_id
FROM customers c
LEFT JOIN accounts a ON c.id = a.customer_id
WHERE c.segment IN ('SMB', 'Corporate', 'Consumer') AND c.country = 'UK'
