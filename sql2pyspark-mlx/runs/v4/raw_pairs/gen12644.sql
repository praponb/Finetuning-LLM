WITH CustomerStats AS (
    SELECT customer_id, SUM(CASE WHEN status = 'OPEN' THEN balance ELSE 0 END) AS open_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT c.id, c.city, C.open_balance
FROM customers c
INNER JOIN CustomerStats C ON c.id = C.customer_id
ORDER BY C.open_balance, city DESC;
