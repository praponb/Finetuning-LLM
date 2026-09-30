WITH valid_customers AS (
    SELECT *, LOWER(name) AS name_lower
    FROM customers
    WHERE email NOT LIKE '%@outlook.com'
),
agg AS (
    SELECT
        city,
        country,
        MIN(signup_date) AS min_signup_date,
        SUM(CASE WHEN segment = 'Corporate' THEN credit_limit ELSE 0 END) AS corporate_credit_limit
    FROM valid_customers
    GROUP BY city, country
)
SELECT
    city,
    country,
    min_signup_date,
    corporate_credit_limit
FROM agg
WHERE corporate_credit_limit < 61
ORDER BY city ASC, country ASC
