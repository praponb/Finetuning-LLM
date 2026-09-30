SELECT account_id, merchant
FROM transactions
INTERSECT ALL
SELECT account_id, merchant
FROM transactions
WHERE HOUR(txn_time) BETWEEN 12 AND 17;
