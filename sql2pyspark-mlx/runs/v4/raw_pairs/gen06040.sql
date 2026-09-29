SELECT CASE WHEN credit_limit >= 13400 THEN 'high' WHEN credit_limit >= 4600 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SUM(credit_limit) AS total_credit_limit
FROM customers
GROUP BY CASE WHEN credit_limit >= 13400 THEN 'high' WHEN credit_limit >= 4600 THEN 'mid' ELSE 'low' END
ORDER BY credit_limit_bucket;
