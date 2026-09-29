WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        INITCAP(TRIM(country)) AS country_title,
        credit_limit + age AS credit_limit_plus_age
    FROM customers
    WHERE signup_date > TO_DATE('2023-02-16')
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.subscription_start_date,
        x.plan
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.start_date AS subscription_start_date, c.plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_running AS credit_limit_plus_age_bucket,
        LAST_VALUE(plan) IGNORE NULLS OVER (PARTITION BY country, segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_plan,
        LAG(credit_limit_plus_age, 1) OVER (PARTITION BY plan ORDER BY credit_limit_plus_age ASC, id ASC) AS prev_credit_limit_plus_age
    FROM latest_match
    WHERE email LIKE '%@company.co'
    WINDOW w_running AS (PARTITION BY country ORDER BY credit_limit_plus_age DESC, id ASC)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        name,
        country,
        segment,
        signup_date,
        credit_limit_plus_age,
        credit_limit_plus_age_bucket,
        last_known_plan,
        prev_credit_limit_plus_age
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY credit_limit_plus_age DESC, id ASC) = 1
)
-- Multi-level aggregation
SELECT
    country,
    segment,
    APPROX_PERCENTILE(prev_credit_limit_plus_age, 0.9) AS prev_credit_limit_plus_age_p90,
    APPROX_PERCENTILE(credit_limit_plus_age_bucket, 0.5) AS credit_limit_plus_age_bucket_p50,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(credit_limit_plus_age_bucket, 0.9) AS credit_limit_plus_age_bucket_p90
FROM latest
GROUP BY 1, 2
ORDER BY row_count ASC NULLS LAST
