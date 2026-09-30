SELECT
    is_active,
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    country,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_th
FROM customers
WHERE is_active = TRUE AND name IS NOT NULL
GROUP BY is_active, signup_month, country
ORDER BY country DESC, pct_th DESC, is_active, signup_month
LIMIT 5
