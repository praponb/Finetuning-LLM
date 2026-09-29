WITH src AS (
    SELECT *
    FROM customers
    WHERE segment NOT IN ('SMB', 'Consumer')
),
agg AS (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, COUNT(*) AS customer_count, MAX(signup_date) AS latest_signup_date
    FROM src
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
)
SELECT signup_month, customer_count, latest_signup_date
FROM agg
WHERE customer_count < 4
ORDER BY customer_count, signup_month
LIMIT 20
