SELECT c.id, c.name, c.country
FROM customers c
WHERE c.id IN (SELECT customer_id FROM accounts WHERE account_type = 'LOAN')
ORDER BY c.country, id
LIMIT 50;
