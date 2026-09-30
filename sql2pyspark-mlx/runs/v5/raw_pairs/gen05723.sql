SELECT
    transactions.id,
    transactions.account_id,
    accounts.status,
    accounts.balance
FROM transactions
FULL OUTER JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.txn_type IN ('FEE', 'CREDIT') AND transactions.txn_type = 'FEE'
ORDER BY transactions.id DESC, balance DESC;
