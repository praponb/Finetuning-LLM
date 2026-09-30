SELECT
    transactions.id,
    transactions.amount,
    transactions.is_flagged,
    transactions.account_id,
    accounts.opened_date,
    accounts.status
FROM transactions
LEFT JOIN accounts ON accounts.id = transactions.account_id
WHERE accounts.opened_date < '2025-11-01';
