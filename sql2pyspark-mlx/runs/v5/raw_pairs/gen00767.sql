WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) AS latest_rank
    FROM customers
    QUALIFY latest_rank = 1
),
scored AS (
    SELECT
        id,
        name,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        COUNT(credit_limit) OVER (PARTITION BY city, segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_credit_limit,
        MIN(credit_limit) OVER (PARTITION BY city, segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY city, segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_credit_limit,
        LEAD(age, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS next_age
    FROM latest
    WHERE latest_rank >= 1
),
base AS (
    SELECT
        id,
        name,
        city,
        signup_date,
        credit_limit,
        age,
        running_count_credit_limit,
        running_min_credit_limit,
        running_sum_credit_limit,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        COALESCE(city, 'NONE') AS city_filled
    FROM scored
    WHERE signup_date >= DATE '2023-03-23'
),
with_windows AS (
    SELECT
        *,
        LEAD(running_min_credit_limit, 1, 0) OVER (ORDER BY credit_limit_filled, id) AS next_running_min_credit_limit,
        RANK() OVER (PARTITION BY city ORDER BY running_sum_credit_limit DESC NULLS LAST) AS running_sum_credit_limit_rank
    FROM base
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IN (5, 9, 11, 13)
        )
)
SELECT
    id,
    name,
    signup_date,
    credit_limit,
    running_count_credit_limit,
    running_sum_credit_limit,
    credit_limit_filled,
    city_filled,
    next_running_min_credit_limit
FROM eligible
ORDER BY running_sum_credit_limit DESC, credit_limit
