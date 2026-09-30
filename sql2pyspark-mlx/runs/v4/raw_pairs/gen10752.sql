WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(credit_limit, 2) OVER (PARTITION BY country ORDER BY credit_limit, id) AS next_credit_limit,
        NTILE(3) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS credit_limit_bucket
    FROM customers
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
),
with_windows_customers AS (
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
        next_credit_limit,
        credit_limit_bucket,
        row_num,
        AVG(credit_limit_bucket) OVER (PARTITION BY country ORDER BY row_num, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_bucket_avg_3_prior,
        NTILE(4) OVER (PARTITION BY country ORDER BY next_credit_limit DESC, id) AS next_credit_limit_bucket,
        CORR(credit_limit, age) OVER w_hist AS corr_credit_limit_age
    FROM deduped
    WINDOW w_hist AS (PARTITION BY city)
),
with_windows_customers_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS event_seq,
        NTILE(10) OVER (PARTITION BY city, segment ORDER BY credit_limit_bucket_avg_3_prior DESC, id) AS credit_limit_bucket_avg_3_prior_bucket
    FROM with_windows_customers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(next_credit_limit, 1, 0) OVER (PARTITION BY city, segment ORDER BY signup_date, id) AS prev_next_credit_limit,
        RANK() OVER (PARTITION BY segment ORDER BY next_credit_limit DESC) AS next_credit_limit_rank,
        AVG(event_seq) OVER (PARTITION BY segment, city ORDER BY credit_limit_bucket_avg_3_prior_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS event_seq_avg_5
    FROM with_windows_customers_2
),
enriched_customers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY event_seq, id) AS seq_no,
        FIRST_VALUE(credit_limit_bucket) IGNORE NULLS OVER (PARTITION BY country ORDER BY credit_limit_bucket_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit_bucket,
        AVG(credit_limit_bucket) OVER (PARTITION BY segment ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS credit_limit_bucket_avg_5,
        FIRST_VALUE(credit_limit_bucket) IGNORE NULLS OVER (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit_bucket_2
    FROM enriched
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit
FROM enriched_customers
ORDER BY segment NULLS FIRST
