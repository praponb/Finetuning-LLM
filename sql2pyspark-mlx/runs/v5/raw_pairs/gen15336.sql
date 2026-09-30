WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        latency_ms,
        status_code,
        key,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(status_code_high, latency_ms) AS least_status_code_high_latency_ms
    FROM parsed
    WHERE key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        latency_ms,
        status_code,
        key,
        attr_value,
        status_code_high,
        least_status_code_high_latency_ms,
        COALESCE(status_code_high, 0) AS status_code_high_filled,
        COALESCE(key, 'unassigned') AS key_filled
    FROM base
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'region'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        latency_ms,
        key,
        attr_value,
        status_code_high_filled,
        key_filled,
        RANK() OVER (PARTITION BY key ORDER BY status_code_high_filled DESC NULLS LAST) AS status_code_high_filled_rank,
        CUME_DIST() OVER (PARTITION BY key ORDER BY least_status_code_high_latency_ms DESC NULLS FIRST) AS least_status_code_high_latency_ms_cume_dist
    FROM filtered
)
SELECT
    latency_ms,
    attr_value,
    status_code_high_filled,
    key_filled,
    status_code_high_filled_rank,
    least_status_code_high_latency_ms_cume_dist
FROM with_windows
ORDER BY status_code_high_filled_rank
