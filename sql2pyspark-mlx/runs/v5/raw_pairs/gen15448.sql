WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN credit_limit >= 14300 THEN 'High' WHEN credit_limit >= 8200 THEN 'Medium' ELSE 'Low' END AS credit_limit_band
    FROM customers
    WHERE city IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date <= s.signup_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        signup_date,
        is_active,
        credit_limit,
        age,
        LOWER(email) AS email_lower,
        LENGTH(segment) AS segment_length,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM not_flagged
    WHERE city IS NOT NULL
      AND signup_date > DATE '2023-05-23'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(age) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        LAST_VALUE(age) IGNORE NULLS OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age,
        CORR(credit_limit, age) OVER (PARTITION BY country) AS corr_credit_limit_age,
        NTILE(5) OVER (ORDER BY credit_limit, id) AS credit_limit_bucket,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share
    FROM base
    WHERE is_active = FALSE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(age, 0) AS age_filled
    FROM with_windows
    WHERE signup_date > DATE '2023-08-18'
),
base_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM valid
    WHERE signup_date >= DATE '2023-09-15'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY age_share, id) AS age_share_bucket,
        MAX(age_filled) OVER (PARTITION BY country) AS country_max_age_filled,
        AVG(last_known_age) OVER (PARTITION BY country ORDER BY last_known_age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last_known_age_avg_5_prior,
        AVG(cumulative_age) OVER (PARTITION BY country ORDER BY last_known_age, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cumulative_age_avg_2_prior,
        AVG(corr_credit_limit_age) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS corr_credit_limit_age_avg_2
    FROM base_customers
)
SELECT
    id,
    name,
    signup_date,
    credit_limit,
    age,
    email_lower,
    segment_length,
    cumulative_age,
    credit_limit_bucket
FROM scored
ORDER BY signup_date NULLS FIRST, credit_limit;
