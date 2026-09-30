SELECT
    accounts.account_type,
    accounts.customer_id,
    accounts.status,
    transactions.merchant
FROM accounts
LEFT JOIN transactions ON transactions.account_id = accounts.id
WHERE accounts.status NOT IN ('OPEN', 'CLOSED') AND transactions.txn_type = 'CREDIT';
