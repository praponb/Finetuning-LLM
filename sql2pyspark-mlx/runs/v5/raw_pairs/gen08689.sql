SELECT *
FROM (
    SELECT CASE WHEN amount >= 4300 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END amount_bucket, ROUND(AVG(amount), 2) avg_amount, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) debit_amount
    FROM transactions
    GROUP BY CASE WHEN amount >= 4300 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END
) agg
WHERE avg_amount >= 70
