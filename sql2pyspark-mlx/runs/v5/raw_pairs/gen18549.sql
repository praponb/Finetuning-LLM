SELECT id, txn_time, RANK() OVER (ORDER BY txn_time) AS rnk
FROM transactions
WHERE amount BETWEEN 2900 AND 3500;
