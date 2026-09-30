SELECT
    is_active,
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    COUNT(age) AS age_count,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
FROM customers
GROUP BY is_active, DATE_FORMAT(signup_date, 'yyyy-MM')
ORDER BY 2 DESC, age_count DESC;
