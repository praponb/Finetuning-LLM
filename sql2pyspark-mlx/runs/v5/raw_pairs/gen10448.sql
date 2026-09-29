SELECT
    transactions.id,
    transactions.account_id,
    accounts.account_type,
    CASE WHEN accounts.account_type IN ('SAVINGS', 'CREDIT') THEN 'Group A' ELSE 'Other' END account_type_group
FROM transactions
INNER JOIN accounts ON transactions.account_id = accounts.id
ORDER BY account_id DESC;
