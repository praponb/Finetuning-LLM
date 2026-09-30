WITH AccountSummary AS (
    SELECT account_id, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, a.balance, A.min_amount
FROM accounts a
LEFT JOIN AccountSummary A ON a.id = A.account_id
WHERE A.min_amount >= 1900
