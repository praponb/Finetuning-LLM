WITH transactions_by_account AS (
    SELECT account_id, MAX(txn_time) AS latest_txn_time
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 9 AND 15
    GROUP BY account_id
)
SELECT accounts.balance, transactions_by_account.latest_txn_time
FROM accounts
INNER JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
ORDER BY latest_txn_time;
