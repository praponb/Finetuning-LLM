SELECT c.name, a.status
FROM customers c
LEFT JOIN accounts a ON c.id = a.customer_id;
