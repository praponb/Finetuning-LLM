WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_code,
        k,
        attr_value,
        ROUND(latency_ms, 1) AS latency_ms_rounded,
        CAST(status_code AS STRING) AS status_code_num,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM flattened
    WHERE status_code = 420
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        k,
        latency_ms_rounded,
        status_code_num,
        k_clean,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_cume_dist,
        AVG(latency_ms_rounded) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS latency_ms_rounded_avg_2,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank
    FROM valid
    WHERE latency_ms_rounded BETWEEN 60 AND 74
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        status_code,
        k,
        latency_ms_rounded,
        status_code_num,
        k_clean,
        status_code_cume_dist,
        latency_ms_rounded_avg_2,
        status_code_percent_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_cume_dist DESC NULLS FIRST, id, k) AS row_num
        FROM scored
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    status_code,
    k,
    latency_ms_rounded,
    status_code_num,
    k_clean,
    latency_ms_rounded_avg_2
FROM latest
