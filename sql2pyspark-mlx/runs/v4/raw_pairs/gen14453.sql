SELECT CASE WHEN amount >= 4200 THEN 'high' WHEN amount >= 1300 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND((100 * SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shell, MIN(amount) AS min_amount
FROM transactions
GROUP BY amount_bucket;
