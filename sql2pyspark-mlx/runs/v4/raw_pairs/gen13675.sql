SELECT
    CASE WHEN age >= 72 THEN 'high' WHEN age >= 26 THEN 'mid' ELSE 'low' END AS age_bucket,
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    COUNT(*) AS customer_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers
GROUP BY age_bucket, signup_month
