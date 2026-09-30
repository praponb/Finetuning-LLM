SELECT
    transactions.is_flagged,
    transactions.amount,
    transactions.merchant,
    accounts.balance,
    accounts.opened_date
FROM transactions
LEFT JOIN accounts ON transactions.account_id = accounts.id
WHERE accounts.status = 'OPEN' AND YEAR(accounts.opened_date) >= 2023;
