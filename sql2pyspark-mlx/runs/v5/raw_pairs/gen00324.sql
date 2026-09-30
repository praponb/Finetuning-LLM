WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value,
        CAST(log_ts AS DATE) AS log_ts_day
    FROM flattened
    WHERE log_ts >= TIMESTAMP '2025-06-01 06:00:00'
      AND k IN ('version', 'trace', 'region')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        k,
        attr_value,
        log_ts_day,
        LAST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY log_ts_day, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
        ROW_NUMBER() OVER (ORDER BY latency_ms, id, k) AS seq_no,
        NTILE(4) OVER w_grp AS latency_ms_bucket,
        ROW_NUMBER() OVER (ORDER BY log_ts_day, id, k) AS seq_no_2,
        COUNT(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_count_2h
    FROM valid
    WHERE host = 'h3'
    WINDOW w_grp AS (PARTITION BY host ORDER BY latency_ms, id, k)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        k,
        seq_no,
        latency_ms_bucket,
        seq_no_2,
        SUM(latency_ms_count_2h) OVER (PARTITION BY k ORDER BY latency_ms_count_2h, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_latency_ms_count_2h,
        MIN(latency_ms_count_2h) OVER (PARTITION BY k ORDER BY latency_ms_count_2h, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_latency_ms_count_2h,
        MAX(latency_ms_count_2h) OVER (PARTITION BY k ORDER BY latency_ms_count_2h, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_latency_ms_count_2h,
        PERCENT_RANK() OVER (ORDER BY seq_no_2 NULLS LAST) AS seq_no_2_percent_rank
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        seq_no_2,
        running_min_latency_ms_count_2h,
        running_max_latency_ms_count_2h,
        COUNT(seq_no) OVER (PARTITION BY k ORDER BY running_sum_latency_ms_count_2h, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_seq_no,
        MIN(seq_no) OVER (PARTITION BY k ORDER BY running_sum_latency_ms_count_2h, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_seq_no,
        MAX(seq_no) OVER (PARTITION BY k ORDER BY running_sum_latency_ms_count_2h, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_seq_no
    FROM enriched
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        running_min_latency_ms_count_2h,
        last4_min_seq_no,
        last4_max_seq_no,
        LAG(seq_no_2) OVER (PARTITION BY k ORDER BY last4_min_seq_no, id, k) AS prev_seq_no_2,
        LAG(last4_count_seq_no) OVER (PARTITION BY k ORDER BY last4_min_seq_no, id, k) AS prev_last4_count_seq_no,
        LEAD(last4_count_seq_no) OVER (PARTITION BY k ORDER BY last4_min_seq_no, id, k) AS next_last4_count_seq_no,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY last4_min_seq_no, id, k) AS seq_no
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    id,
    k,
    running_min_latency_ms_count_2h,
    last4_min_seq_no,
    AVG(next_last4_count_seq_no) OVER (PARTITION BY k ORDER BY running_min_latency_ms_count_2h, id, k ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_last4_count_seq_no_avg_5,
    AVG(next_last4_count_seq_no) OVER (PARTITION BY k ORDER BY prev_last4_count_seq_no, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_last4_count_seq_no_avg_6_prior,
    ROUND(last4_min_seq_no / SUM(last4_min_seq_no) OVER (PARTITION BY k), 4) AS last4_min_seq_no_share
FROM scored_app_logs;
