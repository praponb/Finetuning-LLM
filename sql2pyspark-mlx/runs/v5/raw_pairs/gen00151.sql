WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        host,
        latency_ms,
        status_code,
        k,
        value,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        latency_ms,
        status_code,
        k,
        value,
        flag_bit2,
        status_code_high,
        latency_ms * flag_bit2 AS latency_ms_times_flag_bit2,
        CASE WHEN latency_ms >= 2616 THEN 'High' WHEN latency_ms >= 1158 THEN 'Medium' ELSE 'Low' END AS latency_ms_band
    FROM unpacked
    WHERE k IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY k ORDER BY status_code) AS status_code_rank,
        CORR(status_code, latency_ms) OVER (PARTITION BY k) AS corr_status_code_latency_ms
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        k,
        value,
        status_code_high,
        latency_ms_times_flag_bit2,
        status_code_rank,
        corr_status_code_latency_ms,
        SUM(status_code_high) OVER (PARTITION BY k) AS k_sum_status_code_high,
        RANK() OVER (ORDER BY status_code_rank DESC) AS status_code_rank_rank,
        CORR(latency_ms, status_code) OVER (PARTITION BY k) AS corr_latency_ms_status_code
    FROM scored
    WHERE flag_bit2 >= 0
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        status_code_high * status_code_rank_rank AS status_code_high_times_status_code_rank_rank,
        CASE WHEN status_code_rank >= 8 THEN 'High' WHEN status_code_rank >= 4 THEN 'Medium' ELSE 'Low' END AS status_code_rank_band,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM enriched
    WHERE status_code_rank_rank >= 5
)
-- Multi-level aggregation
SELECT
    k,
    CASE WHEN status_code_rank_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS status_code_rank_rank_tier,
    SUM(CASE WHEN k = 'version' THEN corr_latency_ms_status_code ELSE 0 END) AS version_corr_latency_ms_status_code,
    COUNT(DISTINCT k) AS distinct_ks
FROM base
GROUP BY GROUPING SETS ((k, CASE WHEN status_code_rank_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END), (k))
ORDER BY distinct_ks DESC NULLS FIRST
