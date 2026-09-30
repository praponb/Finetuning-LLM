WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        SUM(credit_limit) OVER w_ordered cumulative_credit_limit,
        SUM(age) OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_age,
        NTILE(10) OVER (PARTITION BY country ORDER BY credit_limit DESC, id) credit_limit_bucket,
        FIRST_VALUE(city, TRUE) OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_city
    FROM customers
    WINDOW w_ordered AS (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date >= s.signup_date
        )
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.plan,
        x.subscription_start_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.plan, c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        is_active,
        credit_limit,
        age,
        cumulative_credit_limit,
        running_age,
        plan,
        CASE WHEN age >= 73 THEN 'High' WHEN age >= 33 THEN 'Medium' ELSE 'Low' END age_tier,
        COALESCE(segment, 'N/A') segment_clean
    FROM with_last
    WHERE plan IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.monthly_fee <= 240
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(credit_limit * 0.9, 2) adjusted_credit_limit,
        ROUND(cumulative_credit_limit, 1) cumulative_credit_limit_rounded
    FROM not_flagged
    WHERE running_age BETWEEN 198 AND 285
)
-- Ranking and gap features
SELECT
    *,
    LAG(credit_limit, 1, 0) OVER w_grp prev_credit_limit,
    SUM(cumulative_credit_limit_rounded) OVER (PARTITION BY plan ORDER BY running_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cumulative_credit_limit_rounded
FROM base
WINDOW w_grp AS (ORDER BY age, id);
