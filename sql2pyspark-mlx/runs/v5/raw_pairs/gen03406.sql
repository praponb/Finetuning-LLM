WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age ASC, id ASC) AS event_seq,
        LAST_VALUE(age) IGNORE NULLS OVER w_ordered AS last_known_age,
        NTILE(10) OVER w_grp AS age_bucket
    FROM customers
    WHERE email LIKE '%@startup.io'
    WINDOW w_ordered AS (ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY age ASC, id ASC)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        event_seq,
        last_known_age,
        age_bucket,
        LEAD(age_bucket) OVER (PARTITION BY city ORDER BY age_bucket ASC, id ASC) AS next_age_bucket,
        LAG(age_bucket) OVER (PARTITION BY city ORDER BY age_bucket ASC, id ASC) AS prev_age_bucket,
        LEAD(credit_limit) OVER (PARTITION BY city ORDER BY age_bucket ASC, id ASC) AS next_credit_limit,
        LAG(credit_limit) OVER (PARTITION BY city ORDER BY age_bucket ASC, id ASC) AS prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age_bucket ASC, id ASC) AS seq_no
    FROM enriched
),
with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.subscription_start_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(seq_no) OVER w_recent AS last7_avg_seq_no,
        MAX(seq_no) OVER w_recent AS last7_max_seq_no,
        SUM(seq_no) OVER w_recent AS last7_sum_seq_no
    FROM with_latest
    WINDOW w_recent AS (PARTITION BY city ORDER BY prev_credit_limit ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        segment,
        country,
        next_credit_limit
    FROM scored
) src
PIVOT (
    SUM(next_credit_limit) AS amount,
    COUNT(1) AS item_count
    FOR country IN ('UK' AS uk, 'SG' AS sg, 'US' AS us)
);
