SELECT id, customer_id
FROM accounts
WHERE balance BETWEEN 28000 AND 36000
UNION ALL
SELECT id, customer_id
FROM accounts
WHERE opened_date <= '2025-09-01'
ORDER BY customer_id;
