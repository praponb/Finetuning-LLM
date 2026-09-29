WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
gaps AS (
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM lines
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY service ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY session_no DESC, id, key) AS row_num
        FROM sessionized
    ) x
    WHERE row_num = 1
),
totals AS (
    SELECT
        key,
        CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
        service,
        SIZE(COLLECT_SET(key)) AS num_keys,
        COUNT(*) AS row_count,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes
    FROM latest
    GROUP BY CUBE (key, CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, service)
    HAVING row_count > 1
)
SELECT
    key,
    session_no_tier,
    service,
    num_keys,
    row_count,
    span_minutes
FROM totals
ORDER BY session_no_tier DESC, span_minutes
