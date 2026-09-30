SELECT c.id, c.signup_date, c.age
FROM customers c
WHERE c.name NOT LIKE '%Pim%' AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.account_type = 'CREDIT')
ORDER BY c.id DESC;
