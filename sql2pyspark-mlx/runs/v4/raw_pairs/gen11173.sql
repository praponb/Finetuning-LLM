WITH filtered AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        FLOOR(credit_limit) AS credit_limit_whole
    FROM customers
    WHERE signup_date >= TO_DATE('2024-04-18')
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at <= s.signup_date
        )
),
latest AS (
    SELECT *
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY segment, city ORDER BY age DESC NULLS LAST) = 1
),
with_latest AS (
    SELECT s.*, x.country
    FROM latest s
    JOIN LATERAL (
            SELECT c.country
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    SELECT
        *,
        SUM(credit_limit_whole) OVER w_grp AS cumulative_credit_limit_whole,
        AVG(age) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS age_avg_3,
        FIRST_VALUE(credit_limit_whole, TRUE) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit_whole,
        NTILE(10) OVER w_running AS credit_limit_bucket,
        AVG(credit_limit_whole) OVER w_part AS segment_avg_credit_limit_whole
    FROM with_latest
    WINDOW w_grp AS (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY city ORDER BY credit_limit, id), w_part AS (PARTITION BY segment)
)
SELECT
    *,
    STDDEV_SAMP(credit_limit_whole) OVER (PARTITION BY country) AS country_stddev_samp_credit_limit_whole,
    NTILE(5) OVER (PARTITION BY city ORDER BY age, id) AS age_bucket,
    ROW_NUMBER() OVER (ORDER BY first_known_credit_limit_whole DESC, id) AS event_seq
FROM with_windows;
