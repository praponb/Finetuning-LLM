SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    country,
    COUNT(DISTINCT city) AS distinct_cities,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    CORR(credit_limit, age) AS corr_credit_limit_age
FROM customers
GROUP BY activity_month, country
HAVING CORR(credit_limit, age) >= 0
