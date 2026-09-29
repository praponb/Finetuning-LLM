SELECT txn_type
FROM transactions
INTERSECT ALL
SELECT txn_type
FROM transactions
WHERE txn_time >= '2023-08-01';
