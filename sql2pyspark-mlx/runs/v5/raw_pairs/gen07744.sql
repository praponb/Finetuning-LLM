SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM customers
GROUP BY DATE_TRUNC('MONTH', signup_date);
