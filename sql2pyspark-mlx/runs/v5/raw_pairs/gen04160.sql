SELECT merchant, account_id
FROM transactions
INTERSECT ALL
SELECT merchant, account_id
FROM transactions
WHERE txn_type = 'CREDIT';
