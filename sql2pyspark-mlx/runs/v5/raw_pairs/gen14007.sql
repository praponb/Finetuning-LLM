SELECT
    city,
    ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok,
    MAX(signup_date) AS latest_signup_date,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers
WHERE email LIKE '%@mail.co.th'
GROUP BY city;
