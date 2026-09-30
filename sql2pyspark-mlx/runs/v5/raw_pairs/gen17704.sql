SELECT customer_id, ROUND(balance, 0) AS balance_rounded, CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END AS is_open
FROM accounts
