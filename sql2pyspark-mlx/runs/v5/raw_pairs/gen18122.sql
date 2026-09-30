WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        country,
        city,
        segment,
        credit_limit,
        age,
        TRIM(email) AS clean_email,
        COALESCE(city, 'OTHER') AS city_clean
    FROM customers
    WHERE segment IS NOT NULL
      AND credit_limit >= 15700
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        segment,
        credit_limit,
        age,
        clean_email,
        city_clean,
        LAG(age) OVER w_part AS prev_age,
        LEAD(age) OVER w_part AS next_age,
        LAG(credit_limit) OVER w_part AS prev_credit_limit,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM filtered
    WHERE age > 62
    WINDOW w_part AS (PARTITION BY city ORDER BY age, id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city_clean,
        prev_age,
        next_age,
        row_seq
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS LAST, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city_clean,
        prev_age,
        row_seq,
        NTILE(10) OVER w_ordered AS row_seq_bucket,
        AVG(prev_age) OVER (ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS prev_age_avg_5,
        ROW_NUMBER() OVER (ORDER BY next_age DESC, id) AS event_seq
    FROM ranked
    WINDOW w_ordered AS (ORDER BY row_seq DESC, id)
),
scored_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(row_seq) OVER w_running AS running_count_row_seq,
        AVG(row_seq) OVER w_running AS running_avg_row_seq,
        MAX(row_seq) OVER w_running AS running_max_row_seq,
        ROW_NUMBER() OVER (ORDER BY event_seq DESC, id) AS event_seq_2
    FROM scored
    WINDOW w_running AS (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    city_clean,
    prev_age,
    row_seq,
    row_seq_bucket,
    prev_age_avg_5,
    event_seq,
    running_avg_row_seq,
    event_seq_2
FROM scored_customers
ORDER BY row_seq DESC NULLS FIRST, prev_age_avg_5;
