WITH RankedTransactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS rn
    FROM transactions
)
SELECT *
FROM RankedTransactions
WHERE rn = 1
ORDER BY account_id DESC;
