SELECT accounts.customer_id, accounts.account_type, t.min_txn_time
FROM accounts
INNER JOIN (SELECT account_id, MIN(txn_time) AS min_txn_time
    FROM transactions
    GROUP BY account_id) t ON accounts.id = t.account_id
ORDER BY customer_id DESC, account_type DESC
