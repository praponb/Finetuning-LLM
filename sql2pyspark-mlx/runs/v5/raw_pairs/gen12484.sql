WITH transactions_by_account AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.id, accounts.status, transactions_by_account.txn_type_values
FROM accounts
INNER JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
ORDER BY accounts.status, id
LIMIT 10;
