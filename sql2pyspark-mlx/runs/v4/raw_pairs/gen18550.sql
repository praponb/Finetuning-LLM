SELECT
    accounts.id,
    accounts.customer_id,
    accounts.status,
    accounts.opened_date
FROM accounts
WHERE EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.merchant != 'Amazon')
