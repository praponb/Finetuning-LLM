WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY balance) AS rn
    FROM accounts
)
SELECT
    id,
    customer_id,
    balance,
    opened_date
FROM numbered
WHERE rn = 1;
