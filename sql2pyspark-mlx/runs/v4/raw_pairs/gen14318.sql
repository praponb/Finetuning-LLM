WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rn
    FROM accounts
)
SELECT
    id,
    customer_id,
    balance,
    opened_date
FROM numbered
WHERE rn = 1
ORDER BY opened_date ASC;
