SELECT
    country,
    QUARTER(signup_date) AS signup_quarter,
    ROUND(STDDEV(age), 2) AS stddev_age,
    SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END) AS corporate_count
FROM customers
WHERE is_active
GROUP BY country, QUARTER(signup_date);
