SELECT a.id, a.status
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE UPPER(TRIM(name)) = 'DAVID LEE');
