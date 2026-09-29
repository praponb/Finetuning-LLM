WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(k, TRUE) OVER (ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_k,
        MAX(status_code) OVER w_recent status_code_max_2h,
        NTILE(3) OVER (ORDER BY status_code DESC, id, k) status_code_bucket
    FROM exploded
    WINDOW w_recent AS (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(status_code_max_2h, TRUE) OVER (PARTITION BY service ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status_code_max_2h,
        PERCENT_RANK() OVER w_ordered status_code_percent_rank,
        AVG(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) status_code_avg_2
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY k, service ORDER BY status_code DESC)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    id,
    service,
    log_ts,
    status_code,
    k,
    last_known_k,
    status_code_max_2h,
    last_known_status_code_max_2h,
    status_code_percent_rank
FROM sessionized
ORDER BY service DESC NULLS LAST
