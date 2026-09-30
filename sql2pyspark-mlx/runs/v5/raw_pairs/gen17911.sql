WITH customer_totals AS (
    SELECT customer_id, MAX(balance) AS max_balance
    FROM accounts
    WHERE status = 'CLOSED'
    GROUP BY customer_id
)
SELECT c.age, ct.max_balance
FROM customers c
INNER JOIN customer_totals ct ON c.id = ct.customer_id;
