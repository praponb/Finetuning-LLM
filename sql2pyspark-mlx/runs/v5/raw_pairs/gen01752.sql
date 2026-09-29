WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        host,
        log_ts,
        status_code,
        key,
        value
    FROM lines
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms DESC, id, key) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code) OVER (ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
        AVG(status_code) OVER (PARTITION BY key, host ORDER BY log_ts, id, key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_avg_2_prior
    FROM ranked
    WHERE host = 'h1'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        key,
        value,
        running_status_code,
        status_code_avg_2_prior,
        NTILE(5) OVER (PARTITION BY key ORDER BY running_status_code, id, key) AS running_status_code_bucket,
        AVG(status_code_avg_2_prior) OVER (PARTITION BY key, host ORDER BY running_status_code, id, key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_avg_2_prior_avg_3_prior,
        SUM(status_code) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_sum_2h
    FROM with_windows
)
SELECT
    id,
    log_ts,
    key,
    value,
    running_status_code,
    status_code_avg_2_prior,
    status_code_avg_2_prior_avg_3_prior,
    status_code_sum_2h
FROM enriched
ORDER BY status_code_avg_2_prior_avg_3_prior;
