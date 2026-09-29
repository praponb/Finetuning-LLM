WITH latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        x.subscription_status,
        x.subscription_end_date
    FROM customers s
    JOIN LATERAL (
            SELECT c.status AS subscription_status, c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(credit_limit) OVER (PARTITION BY city, country ORDER BY subscription_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_credit_limit,
        COUNT(credit_limit) OVER (PARTITION BY city, country ORDER BY subscription_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_credit_limit,
        MIN(credit_limit) OVER (PARTITION BY city, country ORDER BY subscription_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY city, country ORDER BY subscription_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_credit_limit
    FROM latest_match
),
kept_keys AS (
    -- Set difference on keys
    SELECT country
    FROM with_windows
    EXCEPT
    SELECT country
    FROM customers
    WHERE is_active
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.country = k.country
),
with_windows_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        signup_date,
        is_active,
        subscription_status,
        running_sum_credit_limit,
        running_count_credit_limit,
        PERCENT_RANK() OVER (ORDER BY running_count_credit_limit DESC NULLS FIRST) AS running_count_credit_limit_percent_rank,
        PERCENT_RANK() OVER (ORDER BY running_avg_credit_limit DESC NULLS FIRST) AS running_avg_credit_limit_percent_rank
    FROM kept_rows
    WHERE city != 'Singapore'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        subscription_status,
        running_avg_credit_limit_percent_rank,
        running_count_credit_limit_percent_rank,
        running_sum_credit_limit
    FROM with_windows_customers
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (running_avg_credit_limit_percent_rank, running_count_credit_limit_percent_rank AS RUNNING_COUNT_CREDIT_LIMIT_PERCENT_RANK_VAL, running_sum_credit_limit AS RUNNING_SUM_CREDIT_LIMIT)
)
