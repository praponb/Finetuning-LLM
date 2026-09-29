SELECT s.id, s.browser
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE email LIKE '%@gmail.com')
ORDER BY browser, id
LIMIT 10
