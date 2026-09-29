WITH transactions_by_account AS (
    SELECT account_id, MAX(amount) AS max_amount, COUNT(DISTINCT amount) AS unique_txn_types
    FROM transactions
    GROUP BY account_id
)
SELECT
    accounts.id,
    accounts.customer_id,
    accounts.status,
    transactions_by_account.max_amount,
    transactions_by_account.unique_txn_types
FROM accounts
INNER JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
ORDER BY transactions_by_account.unique_txn_types DESC;
