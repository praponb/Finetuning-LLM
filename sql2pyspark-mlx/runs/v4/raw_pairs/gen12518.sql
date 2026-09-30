WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.start_date >= s.signup_date
        )
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND MONTH(ch.opened_date) IN (1, 6, 8)
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM kept_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY credit_limit DESC NULLS LAST, id ASC) = 1
),
latest_customers AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY age ASC) = 1
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN credit_limit >= 17800 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
        CORR(age, credit_limit) AS corr_age_credit_limit,
        PERCENTILE_APPROX(credit_limit, 0.5) AS credit_limit_p50,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS credit_limit_per_th
    FROM latest_customers
    GROUP BY 1
)
SELECT
    credit_limit_tier,
    corr_age_credit_limit,
    credit_limit_p50,
    credit_limit_per_th
FROM summary
ORDER BY credit_limit_per_th DESC;
