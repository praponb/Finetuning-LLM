WITH RecentCustomers AS (
    SELECT *
    FROM customers
    WHERE city != 'Berlin'
),
is_active_summary AS (
    SELECT is_active, DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, ROUND(STDDEV(age), 2) AS stddev_age
    FROM RecentCustomers
    GROUP BY is_active, DATE_FORMAT(signup_date, 'yyyy-MM')
)
SELECT is_active, stddev_age
FROM is_active_summary
WHERE stddev_age < 30
ORDER BY 2 DESC, is_active;
