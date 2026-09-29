SELECT txn_type, total_amount, max_amount
FROM (
    SELECT txn_type, SUM(amount) AS total_amount, MAX(amount) AS max_amount
    FROM transactions
    GROUP BY txn_type
) AS agg
WHERE max_amount < 3700
ORDER BY max_amount DESC;
