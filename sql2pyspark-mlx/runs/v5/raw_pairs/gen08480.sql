WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN latency_ms >= 2314 THEN 'High' WHEN latency_ms >= 577 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        ROUND(latency_ms, 0) AS latency_ms_rounded,
        COALESCE(k, 'N/A') AS k_filled
    FROM lines
    WHERE status_code > 282
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN k = 'version' THEN 1 ELSE 0 END AS is_version,
        CASE WHEN status_code >= 407 THEN 'High' WHEN status_code >= 243 THEN 'Medium' ELSE 'Low' END AS status_code_tier
    FROM base
    WHERE latency_ms < 942
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank,
        RANK() OVER (PARTITION BY k ORDER BY is_version NULLS LAST) AS is_version_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share
    FROM base_app_logs
)
SELECT
    id,
    latency_ms,
    status_code,
    k,
    attr_value,
    latency_ms_level,
    latency_ms_rounded,
    k_filled,
    status_code_tier
FROM scored
ORDER BY id DESC NULLS LAST
