WITH src AS (
    SELECT *
    FROM accounts
    WHERE customer_id IN (10, 21, 30, 31)
),
summary AS (
    SELECT customer_id, MAX(balance) AS max_balance
    FROM src
    GROUP BY customer_id
)
SELECT customer_id, max_balance
FROM summary
WHERE max_balance >= 53000
ORDER BY 1;
