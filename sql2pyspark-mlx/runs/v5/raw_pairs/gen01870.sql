WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS row_num
    FROM transactions
    WHERE account_id = 18
)
SELECT
    id,
    merchant,
    amount,
    account_id
FROM numbered
WHERE row_num <= 3
ORDER BY account_id ASC;
