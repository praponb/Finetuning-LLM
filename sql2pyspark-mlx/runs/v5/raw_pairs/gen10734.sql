SELECT *
FROM (
    SELECT CASE WHEN amount >= 4300 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END AS amount_bucket, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    WHERE txn_time >= '2025-06-21'
    GROUP BY CASE WHEN amount >= 4300 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END
) AS x
WHERE amount_range > 43;
