SELECT id, opened_date, customer_id
FROM accounts
WHERE customer_id IN (12, 15, 20, 30)
ORDER BY opened_date, id
LIMIT 1;
