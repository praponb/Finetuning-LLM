WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN k IN ('region', 'trace') THEN 'Priority' ELSE 'Other' END AS k_group,
        CASE WHEN host = 'h4' THEN 1 ELSE 0 END AS is_h4,
        COALESCE(k, 'N/A') AS k_filled
    FROM flattened
    WHERE k IS NOT NULL
      AND service = 'billing'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(latency_ms, 0) AS latency_ms_filled,
        ROUND(is_h4 * 100, 2) AS adjusted_is_h4,
        COALESCE(k, 'N/A') AS resolved_k
    FROM base
    WHERE host = 'h4'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(k) AS clean_k
    FROM valid
    WHERE k IS NOT NULL
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        service,
        latency_ms,
        status_code,
        k_group,
        is_h4,
        k_filled,
        latency_ms_filled,
        adjusted_is_h4,
        resolved_k,
        clean_k,
        CAST(adjusted_is_h4 AS INT) AS adjusted_is_h4_int,
        COALESCE(k, 'OTHER') AS k_filled_2
    FROM filtered
    WHERE k IS NOT NULL
      AND k = 'trace'
)
-- Running and moving aggregates
SELECT
    service,
    status_code,
    is_h4,
    k_filled,
    resolved_k,
    MIN(adjusted_is_h4) OVER (PARTITION BY service) AS service_min_adjusted_is_h4,
    PERCENT_RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_percent_rank
FROM valid_app_logs
WHERE latency_ms_filled <= 20
