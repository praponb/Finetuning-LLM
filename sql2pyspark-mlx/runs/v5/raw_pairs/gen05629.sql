SELECT customer_id, total_balance
FROM (
    SELECT customer_id, SUM(balance) AS total_balance
    FROM accounts
    GROUP BY customer_id
) AS t
WHERE total_balance > 175000
ORDER BY customer_id;
