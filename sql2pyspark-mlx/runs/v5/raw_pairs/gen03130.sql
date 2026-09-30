WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', attr_key, host) AS attr_key_host_key,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM exploded
    WHERE status_code > 440
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(status_code / latency_ms, 2) AS status_code_per_latency_ms
    FROM filtered
    WHERE level NOT IN ('WARN', 'DEBUG')
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        v,
        attr_key_host_key,
        GREATEST(latency_ms, status_code_per_latency_ms) AS greatest_latency_ms_status_code_per_latency_ms,
        INITCAP(TRIM(service)) AS service_title
    FROM cleaned
    WHERE attr_key IS NOT NULL
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            level,
            host,
            greatest_latency_ms_status_code_per_latency_ms
        FROM cleaned_app_logs
    ) src
    PIVOT (
        AVG(greatest_latency_ms_status_code_per_latency_ms) AS value_sum,
        COUNT(1) AS n
        FOR host IN ('h3' AS h3, 'h4' AS h4)
    )
)
SELECT
    level,
    h3_n,
    h4_value_sum,
    h4_n
FROM by_type;
