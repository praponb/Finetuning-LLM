WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE country = 'TH'
),
customers_stats AS (
    SELECT YEAR(signup_date) AS signup_year, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
    FROM recent_customers
    GROUP BY YEAR(signup_date)
)
SELECT signup_year, stddev_credit_limit
FROM customers_stats
WHERE stddev_credit_limit > 26;
