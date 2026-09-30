WITH exploded AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY host, key ORDER BY latency_ms) AS latency_ms_percent_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share
    FROM exploded
    WHERE level IN ('DEBUG', 'INFO', 'ERROR')
),
cleaned AS (
    SELECT *, status_code - latency_ms AS status_code_minus_latency_ms
    FROM with_windows
    WHERE status_code > 336
),
with_windows_app_logs AS (
    SELECT
        host,
        latency_ms,
        key,
        value,
        latency_ms_percent_rank,
        status_code_share,
        CUME_DIST() OVER (ORDER BY latency_ms_percent_rank DESC NULLS FIRST) AS latency_ms_percent_rank_cume_dist
    FROM cleaned
),
scored AS (
    SELECT
        *,
        MAX(latency_ms_percent_rank) OVER (PARTITION BY host) AS host_max_latency_ms_percent_rank,
        SUM(latency_ms_percent_rank) OVER (PARTITION BY key) AS key_sum_latency_ms_percent_rank,
        RANK() OVER w_running AS latency_ms_rank,
        PERCENT_RANK() OVER w_part AS latency_ms_percent_rank_percent_rank,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC) AS latency_ms_dense_rank
    FROM with_windows_app_logs
    WINDOW w_running AS (PARTITION BY host, key ORDER BY latency_ms DESC), w_part AS (ORDER BY latency_ms_percent_rank NULLS LAST)
),
valid AS (
    SELECT
        *,
        COALESCE(latency_ms, 0) AS latency_ms_filled,
        latency_ms_percent_rank_percent_rank + latency_ms_percent_rank AS latency_ms_percent_rank_percent_rank_plus_latency_ms_percent_rank,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM scored
    WHERE key IS NOT NULL
      AND latency_ms_percent_rank_percent_rank <= 0.31
)
SELECT
    key,
    latency_ms_rank,
    latency_ms_percent_rank_percent_rank,
    latency_ms_filled,
    latency_ms_percent_rank_percent_rank_plus_latency_ms_percent_rank,
    COUNT(host_max_latency_ms_percent_rank) OVER (PARTITION BY host) AS host_count_host_max_latency_ms_percent_rank
FROM valid
