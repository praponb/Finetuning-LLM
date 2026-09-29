WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY credit_limit DESC NULLS LAST, id) AS rank_in_group
        FROM customers
    ) x
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(age, TRUE) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age,
        NTILE(3) OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_bucket
    FROM latest
    WHERE country = 'DE'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.monthly_fee
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    email,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    rank_in_group
FROM with_last
