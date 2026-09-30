WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
with_windows AS (
    SELECT
        host,
        log_ts,
        latency_ms,
        key,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share
    FROM lines
),
matched_keys AS (
    SELECT host
    FROM with_windows
    EXCEPT
    SELECT host
    FROM app_logs
    WHERE latency_ms BETWEEN 1531 AND 2259
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.host = k.host
),
filtered AS (
    SELECT
        *,
        COALESCE(key, 'unassigned') AS key_filled,
        CASE WHEN latency_ms >= 2447 THEN 'High' WHEN latency_ms >= 1146 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        COALESCE(key, 'NONE') AS key_clean
    FROM kept_rows
    WHERE log_ts > DATE '2025-06-01'
      AND key = 'version'
),
scored AS (
    SELECT
        latency_ms,
        key,
        latency_ms_level,
        key_clean,
        SUM(latency_ms) OVER (PARTITION BY host) AS host_sum_latency_ms,
        SUM(latency_ms_share) OVER w_recent AS latency_ms_share_sum_2h,
        PERCENT_RANK() OVER w_grp AS latency_ms_percent_rank,
        COUNT(latency_ms) OVER w_grp_2 AS key_count_latency_ms
    FROM filtered
    WINDOW w_recent AS (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY latency_ms DESC NULLS LAST), w_grp_2 AS (PARTITION BY key)
)
SELECT
    latency_ms,
    key,
    key_clean,
    host_sum_latency_ms,
    latency_ms_share_sum_2h,
    latency_ms_percent_rank,
    key_count_latency_ms
FROM scored
ORDER BY key NULLS LAST;
