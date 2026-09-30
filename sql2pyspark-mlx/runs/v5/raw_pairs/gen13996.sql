WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM exploded
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status_code,
        key,
        attr_value,
        status_code_high,
        is_flagged
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY latency_ms, id, key) AS latest_rank
        FROM flattened_attrs
    ) ranked
    WHERE latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code, id, key) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY rn DESC, id, key) AS row_seq,
        SUM(status_code) OVER (PARTITION BY key ORDER BY rn, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        SUM(status_code_high) OVER (PARTITION BY key ORDER BY rn, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_high,
        SUM(status_code_high) OVER (PARTITION BY key ORDER BY is_flagged, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_high,
        SUM(is_flagged) OVER (ORDER BY rn, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_flagged
    FROM deduped
)
-- Multi-level aggregation
SELECT
    key,
    ROUND(SUM(cumulative_status_code_high) / NULLIF(COUNT(CASE WHEN key = 'version' THEN 1 END), 0), 2) AS cumulative_status_code_high_per_version,
    COUNT_IF(key = 'trace') AS trace_events
FROM enriched
GROUP BY key
HAVING cumulative_status_code_high_per_version < 1000000
