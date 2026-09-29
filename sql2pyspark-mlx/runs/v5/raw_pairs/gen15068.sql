WITH RecentCustomers AS (
    SELECT *, TRIM(name) AS clean_name
    FROM customers
    WHERE NOT is_active
),
summary AS (
    SELECT QUARTER(signup_date) AS signup_quarter, MAX(age) AS max_age, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
    FROM RecentCustomers
    GROUP BY QUARTER(signup_date)
)
SELECT signup_quarter, credit_limit_range
FROM summary
WHERE credit_limit_range >= 44
