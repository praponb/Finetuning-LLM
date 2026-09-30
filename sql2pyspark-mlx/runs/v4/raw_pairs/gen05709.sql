SELECT
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    segment,
    MIN(age) AS min_age,
    MAX(signup_date) AS last_signup_date,
    MAX(signup_date) AS latest_signup_date
FROM customers
WHERE LENGTH(name) < 6 AND name IS NULL AND YEAR(signup_date) >= 2025
GROUP BY signup_month, segment
HAVING MIN(age) < 67
ORDER BY latest_signup_date ASC, signup_month ASC, segment ASC
LIMIT 1;
