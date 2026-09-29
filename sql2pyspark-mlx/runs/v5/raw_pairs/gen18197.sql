WITH base AS (
    SELECT
        id,
        country,
        city,
        is_active,
        credit_limit,
        age,
        GREATEST(age, credit_limit) AS greatest_age_credit_limit,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM customers
    WHERE signup_date > TO_DATE('2023-09-24')
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.end_date BETWEEN '2024-04-18' AND '2024-11-15'
        )
),
not_flagged_customers AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND YEAR(x.opened_date) = 2023
        )
),
not_flagged_customers_2 AS (
    SELECT s.*
    FROM not_flagged_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.device = 'mobile'
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged_customers_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.amount < 480
        )
),
totals AS (
    SELECT
        country,
        SUM(CASE WHEN country = 'TH' THEN age ELSE 0 END) AS th_age,
        STDDEV_SAMP(age) AS age_stddev,
        ROUND(SUM(days_since_signup_date) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS days_since_signup_date_per_new_york,
        APPROX_PERCENTILE(days_since_signup_date, ARRAY(0.25, 0.5, 0.75)) AS days_since_signup_date_quartiles,
        COUNT(*) AS num_rows,
        COUNT(*) AS num_rows_2
    FROM eligible
    GROUP BY country
)
SELECT country, days_since_signup_date_per_new_york
FROM totals
ORDER BY country
LIMIT 5;
