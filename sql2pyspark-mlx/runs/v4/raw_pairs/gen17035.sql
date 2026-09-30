WITH Base AS (
    SELECT *
    FROM accounts
    WHERE opened_date > '2025-09-01' OR status IN ('CLOSED', 'OPEN')
),
summary AS (
    SELECT customer_id, ROUND(AVG(balance), 2) AS avg_balance, MIN(opened_date) AS min_opened_date
    FROM Base
    GROUP BY customer_id
)
SELECT *
FROM summary
WHERE avg_balance > 39;
