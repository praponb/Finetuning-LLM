SELECT
    transactions.txn_time,
    accounts.balance,
    accounts.status,
    customers.credit_limit
FROM transactions
LEFT OUTER JOIN accounts ON transactions.account_id = accounts.id
FULL OUTER JOIN customers ON accounts.customer_id = customers.id
WHERE transactions.amount > 3600
ORDER BY accounts.status DESC;
