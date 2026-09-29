SELECT customer_id, MIN(balance) AS min_balance
FROM accounts
WHERE customer_id IS NOT NULL AND opened_date < '2023-05-01'
GROUP BY 1;
