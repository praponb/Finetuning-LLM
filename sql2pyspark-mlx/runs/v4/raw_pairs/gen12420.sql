SELECT
    CASE WHEN age >= 71 THEN 'high' WHEN age >= 36 THEN 'mid' ELSE 'low' END AS age_bucket,
    SUM(credit_limit) AS total_credit_limit,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
GROUP BY age_bucket
ORDER BY 2, stddev_credit_limit;
