WITH TransactionsByAccount AS (
    SELECT account_id, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.status,
    T.min_amount
FROM accounts a
LEFT JOIN TransactionsByAccount T ON a.id = T.account_id
WHERE T.min_amount >= 1800
ORDER BY id DESC;
