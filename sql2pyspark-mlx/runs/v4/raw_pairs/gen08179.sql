SELECT SORT_ARRAY(COLLECT_SET(country)) AS country_list, COUNT(credit_limit) AS credit_limit_count
FROM customers
WHERE country = 'DE'
ORDER BY credit_limit_count DESC
