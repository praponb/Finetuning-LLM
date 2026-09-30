WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        country,
        city,
        signup_date,
        credit_limit,
        age,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        age + credit_limit AS age_plus_credit_limit,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM customers
    WHERE signup_date >= DATE '2023-03-01'
      AND segment IS DISTINCT FROM 'SMB'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        credit_limit,
        age_plus_credit_limit,
        city_filled,
        MAX(credit_limit) OVER w_hist AS running_max_credit_limit,
        AVG(credit_limit) OVER w_hist AS running_avg_credit_limit,
        MIN(credit_limit) OVER w_hist AS running_min_credit_limit,
        SUM(age_plus_credit_limit) OVER w_part AS cumulative_age_plus_credit_limit
    FROM base
    WHERE city = 'Berlin'
    WINDOW w_hist AS (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(running_min_credit_limit * 1.07, 2) AS running_min_credit_limit_scaled,
        running_avg_credit_limit - running_max_credit_limit AS running_avg_credit_limit_minus_running_max_credit_limit
    FROM scored
    WHERE country IN ('TH', 'JP', 'SG')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_max_credit_limit, 1, 0) OVER (PARTITION BY country ORDER BY age_plus_credit_limit, id) AS prev_running_max_credit_limit,
        LEAD(running_avg_credit_limit, 1) OVER w_grp AS next_running_avg_credit_limit,
        FIRST_VALUE(running_avg_credit_limit) IGNORE NULLS OVER (PARTITION BY country ORDER BY running_avg_credit_limit_minus_running_max_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_credit_limit
    FROM filtered
    WINDOW w_grp AS (PARTITION BY country ORDER BY running_max_credit_limit, id)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.browser NOT IN ('Edge', 'Chrome')
        )
)
-- Running and moving aggregates
SELECT
    id,
    credit_limit,
    city_filled,
    running_avg_credit_limit,
    running_min_credit_limit,
    prev_running_max_credit_limit,
    next_running_avg_credit_limit,
    first_known_running_avg_credit_limit,
    MAX(age_plus_credit_limit) OVER (PARTITION BY country) AS country_max_age_plus_credit_limit,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY running_avg_credit_limit_minus_running_max_credit_limit DESC NULLS FIRST) AS running_avg_credit_limit_minus_running_max_credit_limit_percent_rank,
    FIRST_VALUE(age_plus_credit_limit) IGNORE NULLS OVER (PARTITION BY country ORDER BY next_running_avg_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age_plus_credit_limit
FROM not_flagged;
