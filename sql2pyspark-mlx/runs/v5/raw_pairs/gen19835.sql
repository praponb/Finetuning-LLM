SELECT CASE WHEN credit_limit >= 16600 THEN 'high' WHEN credit_limit >= 3500 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, ROUND((100 * SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_tokyo
FROM customers
WHERE LENGTH(name) < 13
GROUP BY CASE WHEN credit_limit >= 16600 THEN 'high' WHEN credit_limit >= 3500 THEN 'mid' ELSE 'low' END
ORDER BY credit_limit_bucket
