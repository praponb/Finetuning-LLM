SELECT c.id, c.age, c.city
FROM customers c
WHERE LENGTH(c.name) > 7 AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id)
ORDER BY c.city DESC, c.id
