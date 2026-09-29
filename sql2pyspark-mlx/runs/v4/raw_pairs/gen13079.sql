SELECT id
FROM transactions
EXCEPT
SELECT id
FROM transactions
WHERE HOUR(txn_time) BETWEEN 10 AND 15
