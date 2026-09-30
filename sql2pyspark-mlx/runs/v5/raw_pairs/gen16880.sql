WITH exploded AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
base AS (
    SELECT *, UPPER(key) AS key_upper
    FROM exploded
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
),
cleaned AS (
    SELECT
        log_ts,
        v,
        key_upper,
        ROUND(status_code / latency_ms, 2) AS status_code_per_latency_ms,
        CAST(status_code AS DOUBLE) AS status_code_num,
        COALESCE(key, 'N/A') AS resolved_key
    FROM base
    WHERE log_ts > DATE '2025-06-01'
      AND key = 'trace'
),
cleaned_app_logs AS (
    SELECT
        v,
        status_code_per_latency_ms,
        status_code_num,
        resolved_key,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM cleaned
    WHERE status_code_num <= 36
),
metrics AS (
    SELECT
        v,
        status_code_num,
        resolved_key,
        CUME_DIST() OVER (ORDER BY status_code_per_latency_ms ASC) AS status_code_per_latency_ms_cume_dist
    FROM cleaned_app_logs
),
enriched AS (
    SELECT *, RANK() OVER w_part AS status_code_per_latency_ms_cume_dist_rank
    FROM metrics
    WINDOW w_part AS (ORDER BY status_code_per_latency_ms_cume_dist DESC)
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code_per_latency_ms_cume_dist DESC NULLS LAST) AS status_code_per_latency_ms_cume_dist_cume_dist,
        PERCENT_RANK() OVER (ORDER BY status_code_num ASC NULLS LAST) AS status_code_num_percent_rank
    FROM enriched
)
SELECT
    CASE WHEN status_code_num >= 87 THEN 'HIGH' ELSE 'LOW' END AS status_code_num_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(status_code_per_latency_ms_cume_dist_rank, 0.95) AS status_code_per_latency_ms_cume_dist_rank_p95,
    PERCENTILE_APPROX(status_code_per_latency_ms_cume_dist_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_per_latency_ms_cume_dist_rank_quartiles,
    SUM(status_code_per_latency_ms_cume_dist_rank) AS total_status_code_per_latency_ms_cume_dist_rank
FROM scored
GROUP BY CASE WHEN status_code_num >= 87 THEN 'HIGH' ELSE 'LOW' END
ORDER BY total_status_code_per_latency_ms_cume_dist_rank ASC NULLS FIRST
