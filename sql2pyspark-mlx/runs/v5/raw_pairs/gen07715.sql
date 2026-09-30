SELECT
    CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 2700 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    SUM(SUM(age)) OVER (ORDER BY CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 2700 THEN 'mid' ELSE 'low' END) AS cumulative_age
FROM customers
GROUP BY CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 2700 THEN 'mid' ELSE 'low' END
ORDER BY credit_limit_bucket DESC, cnt DESC;
