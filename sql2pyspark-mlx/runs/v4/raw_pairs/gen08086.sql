WITH customer_stats AS (
    SELECT customer_id, SUM(CASE WHEN status = 'OPEN' THEN balance ELSE 0 END) AS open_balance, MAX(balance) AS max_balance
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN')
    GROUP BY customer_id
)
SELECT c.age, cs.open_balance, cs.max_balance
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id;
