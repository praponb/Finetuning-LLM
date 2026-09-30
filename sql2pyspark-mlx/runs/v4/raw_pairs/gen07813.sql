WITH with_windows AS (
    -- Running and moving aggregates
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
        LEAD(credit_limit, 2) OVER w_running AS next_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS row_seq,
        NTILE(5) OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM customers
    WINDOW w_running AS (PARTITION BY segment ORDER BY signup_date, id)
),
metrics AS (
    -- Running and moving aggregates
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
        next_credit_limit,
        row_seq,
        credit_limit_bucket,
        CUME_DIST() OVER w_grp AS age_cume_dist,
        STDDEV_SAMP(next_credit_limit) OVER (PARTITION BY country) AS country_stddev_samp_next_credit_limit,
        SUM(next_credit_limit) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_credit_limit,
        DENSE_RANK() OVER w_ordered AS next_credit_limit_dense_rank,
        MAX(credit_limit) OVER (PARTITION BY city) AS city_max_credit_limit
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY segment ORDER BY age NULLS FIRST), w_ordered AS (PARTITION BY segment ORDER BY next_credit_limit NULLS LAST)
),
enriched AS (
    -- Ranking and gap features
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
        next_credit_limit,
        row_seq,
        credit_limit_bucket,
        age_cume_dist,
        country_stddev_samp_next_credit_limit,
        cumulative_next_credit_limit,
        next_credit_limit_dense_rank,
        city_max_credit_limit,
        SUM(credit_limit) OVER (ORDER BY credit_limit_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        STDDEV_SAMP(cumulative_next_credit_limit) OVER (PARTITION BY country) AS country_stddev_samp_cumulative_next_credit_limit,
        NTILE(10) OVER (PARTITION BY city, country ORDER BY credit_limit_bucket, id) AS credit_limit_bucket_bucket,
        NTILE(3) OVER (PARTITION BY country ORDER BY row_seq DESC, id) AS row_seq_bucket,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS row_seq_2
    FROM metrics
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY age_cume_dist DESC NULLS LAST, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.account_balance
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.balance AS account_balance
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked_customers AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY running_credit_limit) AS rn
        FROM latest_match
    ) x
    WHERE rn = 1
),
with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.start_date
    FROM ranked_customers s
    LEFT JOIN LATERAL (
            SELECT c.start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    signup_date,
    is_active,
    age,
    credit_limit_bucket,
    CASE WHEN segment RLIKE '^(Consumer|SMB)$' THEN 'PRIORITY' WHEN segment RLIKE '^(Corporate)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS segment_zone
FROM with_latest
