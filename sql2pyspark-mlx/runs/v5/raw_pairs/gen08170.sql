WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        paid_at,
        COALESCE(method, 'unassigned') AS method_filled,
        CONCAT_WS('-', status, method) AS status_method_key,
        COALESCE(method, 'OTHER') AS method_filled_2
    FROM payments
    WHERE method IS DISTINCT FROM 'promptpay'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        paid_at,
        method_filled,
        method_filled_2,
        ROW_NUMBER() OVER w_hist AS seq_no,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER w_hist_2 AS seq_no_2
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY method ORDER BY paid_at, id), w_running AS (ORDER BY paid_at DESC, id), w_hist_2 AS (ORDER BY paid_at, id)
),
with_windows_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        paid_at,
        method_filled,
        NTILE(10) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket,
        LEAD(event_seq, 2) OVER (PARTITION BY method ORDER BY seq_no, id) AS next_event_seq,
        CUME_DIST() OVER (ORDER BY seq_no DESC) AS seq_no_cume_dist,
        MAX(seq_no) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS seq_no_max_30d
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        seq_no_cume_dist,
        seq_no_max_30d,
        LAG(seq_no_bucket) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_seq_no_bucket,
        LEAD(next_event_seq) OVER (PARTITION BY method ORDER BY paid_at, id) AS next_next_event_seq,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS event_seq
    FROM with_windows_payments
    WHERE seq_no_cume_dist < 0.23
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        next_next_event_seq * event_seq AS next_next_event_seq_times_event_seq,
        seq_no_cume_dist - next_next_event_seq AS seq_no_cume_dist_minus_next_next_event_seq
    FROM scored
    WHERE method IS NOT DISTINCT FROM 'paypal'
      AND method = 'paypal'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        seq_no_cume_dist,
        seq_no_max_30d,
        next_next_event_seq,
        event_seq,
        next_next_event_seq_times_event_seq,
        seq_no_cume_dist_minus_next_next_event_seq,
        CASE WHEN prev_seq_no_bucket >= 8 THEN 'High' WHEN prev_seq_no_bucket >= 2 THEN 'Medium' ELSE 'Low' END AS prev_seq_no_bucket_tier,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM cleaned_payments
    WHERE method IS NOT NULL
),
report AS (
    SELECT
        id,
        seq_no_max_30d,
        next_next_event_seq,
        event_seq,
        next_next_event_seq_times_event_seq,
        seq_no_cume_dist_minus_next_next_event_seq,
        resolved_method
    FROM base
)
SELECT
    id,
    seq_no_max_30d,
    next_next_event_seq_times_event_seq,
    seq_no_cume_dist_minus_next_next_event_seq,
    resolved_method
FROM report
ORDER BY next_next_event_seq_times_event_seq NULLS FIRST, seq_no_max_30d;
