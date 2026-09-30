WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
base AS (
    SELECT
        *,
        ROUND(status_code * 1.15, 2) AS status_code_adjusted,
        SUBSTRING(level, 1, 3) AS level_prefix
    FROM lines
    WHERE log_ts >= DATE '2025-06-01'
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS seq_no,
        AVG(status_code) OVER (ORDER BY status_code_adjusted, id, key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_code_avg_5
    FROM base
    WINDOW w_running AS (PARTITION BY level ORDER BY status_code_adjusted DESC, id, key)
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY host, key ORDER BY log_ts, id, key) AS row_seq,
        AVG(status_code_avg_5) OVER (PARTITION BY key) AS key_avg_status_code_avg_5
    FROM with_windows
)
SELECT
    CASE WHEN status_code_avg_5 >= 429 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_avg_5_tier,
    COUNT(DISTINCT key) AS distinct_keys,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS seq_no_per_debug
FROM scored
GROUP BY CASE WHEN status_code_avg_5 >= 429 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING seq_no_per_debug > 0
ORDER BY status_code_avg_5_tier DESC
