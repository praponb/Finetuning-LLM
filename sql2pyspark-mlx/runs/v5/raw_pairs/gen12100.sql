SELECT *
FROM (
    SELECT YEAR(signup_date) AS signup_year, SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END) AS consumer_count, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM customers
    GROUP BY YEAR(signup_date)
) AS sub
WHERE consumer_count > 41
ORDER BY consumer_count
