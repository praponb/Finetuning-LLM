SELECT txn_type, min_amount, total_count
FROM (
    SELECT txn_type, MIN(amount) min_amount, COUNT(*) total_count
    FROM transactions
    WHERE amount <= 4000
    GROUP BY txn_type
) agg
WHERE total_count < 7
ORDER BY total_count
