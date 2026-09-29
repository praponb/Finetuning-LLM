WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(monthly_fee * 1.07, 2) AS adjusted_monthly_fee
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-09-16')
      AND start_date BETWEEN '2023-07-02' AND '2023-12-26'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        start_date,
        status,
        LAG(monthly_fee) OVER w_part AS prev_monthly_fee,
        LEAD(adjusted_monthly_fee) OVER w_part AS next_adjusted_monthly_fee,
        LAG(adjusted_monthly_fee) OVER w_part AS prev_adjusted_monthly_fee,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM filtered
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY end_date, id), w_grp AS (PARTITION BY status ORDER BY monthly_fee DESC, id)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        start_date,
        next_adjusted_monthly_fee,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY next_adjusted_monthly_fee DESC, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        next_adjusted_monthly_fee,
        seq_no,
        MAX(seq_no) OVER w_running AS running_max_seq_no,
        COUNT(seq_no) OVER w_running AS running_count_seq_no,
        SUM(seq_no) OVER w_running AS running_sum_seq_no
    FROM latest
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        next_adjusted_monthly_fee,
        seq_no,
        running_max_seq_no,
        running_count_seq_no,
        running_sum_seq_no,
        CAST(seq_no AS STRING) AS seq_no_str
    FROM with_windows
    WHERE running_sum_seq_no BETWEEN 18 AND 24
)
SELECT
    id,
    next_adjusted_monthly_fee,
    seq_no,
    running_sum_seq_no,
    seq_no_str
FROM base
ORDER BY next_adjusted_monthly_fee DESC;
