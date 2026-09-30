SELECT txn_type, merchant
FROM transactions
INTERSECT ALL
SELECT txn_type, merchant
FROM transactions
WHERE txn_type != 'DEBIT';
