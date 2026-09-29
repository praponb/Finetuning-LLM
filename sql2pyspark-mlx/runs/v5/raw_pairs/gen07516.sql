SELECT
    segment,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    MAX(age) - MIN(age) AS age_range,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM customers
GROUP BY segment
ORDER BY avg_credit_limit DESC, city_values
