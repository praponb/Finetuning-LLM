WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        log_ts,
        key,
        value,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts DESC, id, key) AS row_seq,
        LEAD(status_code, 1) OVER (PARTITION BY key ORDER BY status_code, id, key) AS next_status_code
    FROM flattened
    WHERE key = 'version'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(next_status_code) OVER (ORDER BY next_status_code, id, key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS next_status_code_avg_3_prior,
        NTILE(3) OVER (PARTITION BY host ORDER BY row_seq, id, key) AS row_seq_bucket,
        ROW_NUMBER() OVER w_running AS row_seq_2
    FROM enriched
    WINDOW w_running AS (ORDER BY next_status_code, id, key)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        key,
        row_seq,
        next_status_code,
        next_status_code_avg_3_prior,
        row_seq_bucket,
        ROUND(row_seq_2, 0) AS row_seq_2_rounded,
        next_status_code + next_status_code_avg_3_prior AS next_status_code_plus_next_status_code_avg_3_prior
    FROM scored
    WHERE log_ts >= TIMESTAMP '2025-06-01 13:00:00'
      AND row_seq BETWEEN 5 AND 7
)
-- Grouped metrics
SELECT
    CASE WHEN next_status_code_plus_next_status_code_avg_3_prior >= 81 THEN 'LARGE' ELSE 'SMALL' END AS next_status_code_plus_next_status_code_avg_3_prior_tier,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    PERCENTILE_APPROX(next_status_code_plus_next_status_code_avg_3_prior, 0.95) AS next_status_code_plus_next_status_code_avg_3_prior_p95,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count,
    MIN(log_ts) AS first_log_ts,
    SUM(CASE WHEN host = 'h4' THEN row_seq_bucket ELSE 0 END) AS h4_row_seq_bucket,
    AVG(row_seq_2_rounded) FILTER (WHERE key = 'region') AS region_avg_row_seq_2_rounded
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN next_status_code_plus_next_status_code_avg_3_prior >= 81 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('DAY', log_ts)), (CASE WHEN next_status_code_plus_next_status_code_avg_3_prior >= 81 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY event_count
