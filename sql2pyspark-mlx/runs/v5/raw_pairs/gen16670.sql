WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY service ORDER BY status_code) AS rn
        FROM flattened
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY key ORDER BY rn) AS rn_2
        FROM ranked
    ) t
    WHERE rn_2 = 1
),
ranked_app_logs AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms NULLS FIRST, id, key) AS latest_rank
        FROM latest
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        key,
        attr_value,
        rn,
        rn_2,
        SUM(latency_ms) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_latency_ms,
        SUM(status_code) OVER (PARTITION BY service, host ORDER BY rn, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_code
    FROM ranked_app_logs
)
SELECT
    id,
    latency_ms,
    status_code,
    key,
    rn,
    rn_2
FROM with_windows;
