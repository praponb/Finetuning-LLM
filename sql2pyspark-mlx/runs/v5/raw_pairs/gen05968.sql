WITH customer_summary AS (
    SELECT customer_id, ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT c.id, c.credit_limit, cs.avg_balance
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.avg_balance > 79
ORDER BY c.id DESC;
