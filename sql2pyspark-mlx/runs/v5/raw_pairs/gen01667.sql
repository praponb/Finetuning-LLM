WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        latency_ms,
        status_code,
        attr_key,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE status_code = 310
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        status_code,
        attr_key,
        has_fault,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share
    FROM unpacked
    WHERE attr_key IN ('trace', 'region')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(status_code_share, 0) AS status_code_share_filled,
        COALESCE(attr_key, 'NONE') AS resolved_attr_key
    FROM enriched
    WHERE status_code > 263
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        status_code,
        status_code_share,
        status_code_share_filled,
        MIN(status_code) OVER (PARTITION BY attr_key) AS attr_key_min_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share_2
    FROM filtered
)
SELECT
    status_code,
    status_code_share,
    status_code_share_filled,
    attr_key_min_status_code,
    status_code_share_2
FROM with_windows
