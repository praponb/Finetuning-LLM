WITH recent_customers AS (
    SELECT *, ROUND(credit_limit / age, 2) AS credit_limit_per_age
    FROM customers
    WHERE credit_limit <= 14500 AND name LIKE '%Mia%'
),
summary AS (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END) AS us_count, SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM recent_customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
)
SELECT signup_month, us_count, segment_list
FROM summary
