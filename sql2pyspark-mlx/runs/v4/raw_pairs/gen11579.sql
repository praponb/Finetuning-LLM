SELECT a.account_type, a.opened_date, t.txn_time
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE a.account_type IN ('CREDIT', 'CHECKING', 'SAVINGS') OR MONTH(a.opened_date) IN (3, 4, 6)
