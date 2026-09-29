SELECT
    QUARTER(signup_date) AS signup_quarter,
    AVG(age) AS avg_age,
    MAX(signup_date) AS latest_signup_date,
    SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END) AS corporate_count
FROM customers
GROUP BY signup_quarter
ORDER BY signup_quarter DESC;
