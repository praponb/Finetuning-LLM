SELECT
    CASE WHEN credit_limit >= 15000 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    AVG(credit_limit) FILTER (WHERE country = 'US') AS us_avg_credit_limit
FROM customers
GROUP BY 1;
