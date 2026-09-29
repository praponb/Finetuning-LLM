WITH customer_summary AS (
    SELECT customer_id, COUNT(balance) AS balance_count, ROUND(STDDEV(balance), 2) AS stddev_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.city,
    cs.balance_count,
    cs.stddev_balance
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY balance_count, city;
