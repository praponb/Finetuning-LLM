WITH FilteredCustomers AS (
    SELECT *
    FROM customers
    WHERE is_active = TRUE AND signup_date < '2025-10-01'
),
customers_stats AS (
    SELECT CASE WHEN credit_limit >= 12900 THEN 'high' WHEN credit_limit >= 3000 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, MIN(signup_date) AS first_signup_date, MIN(signup_date) AS min_signup_date
    FROM FilteredCustomers
    GROUP BY CASE WHEN credit_limit >= 12900 THEN 'high' WHEN credit_limit >= 3000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM customers_stats
ORDER BY credit_limit_bucket
