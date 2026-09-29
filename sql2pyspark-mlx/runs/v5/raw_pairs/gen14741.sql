WITH base AS (
    SELECT *, LOWER(country) AS country_lower
    FROM customers
    WHERE credit_limit IS NOT NULL AND segment = 'SMB'
),
CustomersStats AS (
    SELECT
        CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
        city,
        COUNT(*) AS num_customers,
        COUNT(age) AS age_count
    FROM base
    GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, city
)
SELECT
    signup_cohort,
    city,
    num_customers,
    age_count
FROM CustomersStats
ORDER BY 1;
