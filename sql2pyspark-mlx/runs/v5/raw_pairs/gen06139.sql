WITH transactions_by_account AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_merchants
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.id, accounts.account_type, transactions_by_account.unique_merchants
FROM accounts
LEFT JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
WHERE transactions_by_account.unique_merchants > 4
ORDER BY id DESC;
