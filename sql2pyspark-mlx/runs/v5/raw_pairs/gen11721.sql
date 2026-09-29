WITH lines AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
filtered AS (
    SELECT
        *,
        GREATEST(status_code, latency_ms) AS greatest_status_code_latency_ms,
        COALESCE(k, 'NONE') AS k_filled
    FROM lines
    WHERE k IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        level,
        status_code,
        k,
        attr_value,
        SUM(greatest_status_code_latency_ms) OVER w_grp AS running_sum_greatest_status_code_latency_ms,
        AVG(greatest_status_code_latency_ms) OVER w_grp AS running_avg_greatest_status_code_latency_ms,
        MIN(greatest_status_code_latency_ms) OVER w_grp AS running_min_greatest_status_code_latency_ms
    FROM filtered
    WINDOW w_grp AS (PARTITION BY k ORDER BY greatest_status_code_latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        SUM(running_sum_greatest_status_code_latency_ms) OVER (PARTITION BY level ORDER BY running_sum_greatest_status_code_latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_running_sum_greatest_status_code_latency_ms,
        AVG(running_sum_greatest_status_code_latency_ms) OVER (PARTITION BY level ORDER BY running_sum_greatest_status_code_latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_running_sum_greatest_status_code_latency_ms,
        COUNT(running_sum_greatest_status_code_latency_ms) OVER (PARTITION BY level ORDER BY running_sum_greatest_status_code_latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_running_sum_greatest_status_code_latency_ms
    FROM with_windows
    WHERE running_avg_greatest_status_code_latency_ms BETWEEN 31 AND 53
),
with_windows_app_logs AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY last4_sum_running_sum_greatest_status_code_latency_ms NULLS LAST) AS last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank,
        COUNT(running_avg_greatest_status_code_latency_ms) OVER (PARTITION BY level) AS level_count_running_avg_greatest_status_code_latency_ms,
        ROUND(last4_count_running_sum_greatest_status_code_latency_ms / SUM(last4_count_running_sum_greatest_status_code_latency_ms) OVER (PARTITION BY k), 4) AS last4_count_running_sum_greatest_status_code_latency_ms_share
    FROM scored
)
SELECT
    level,
    COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
    MAX(last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank) FILTER (WHERE level = 'DEBUG') AS debug_max_last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank,
    PERCENTILE_APPROX(last4_avg_running_sum_greatest_status_code_latency_ms, 0.9) AS last4_avg_running_sum_greatest_status_code_latency_ms_p90
FROM with_windows_app_logs
GROUP BY 1
HAVING debug_max_last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank > 0;
