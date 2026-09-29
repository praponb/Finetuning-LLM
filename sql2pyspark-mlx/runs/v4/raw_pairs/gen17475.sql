SELECT
    CASE WHEN credit_limit >= 13700 THEN 'high' WHEN credit_limit >= 2600 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    MONTH(signup_date) AS signup_month,
    COUNT(DISTINCT age) AS unique_countrys,
    MAX(credit_limit) AS max_credit_limit,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers
WHERE NOT is_active AND name IS NOT NULL
GROUP BY CUBE(CASE WHEN credit_limit >= 13700 THEN 'high' WHEN credit_limit >= 2600 THEN 'mid' ELSE 'low' END, MONTH(signup_date))
ORDER BY max_credit_limit DESC
