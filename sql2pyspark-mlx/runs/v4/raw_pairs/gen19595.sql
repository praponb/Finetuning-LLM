WITH exploded AS (
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
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
base AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        v,
        CASE WHEN level = 'WARN' THEN 1 ELSE 0 END AS is_warn,
        CASE WHEN service IN ('search', 'checkout') THEN 'Core' ELSE 'Other' END AS service_group
    FROM exploded
    WHERE attr_key = 'version'
),
filtered AS (
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM base
    WHERE attr_key IS NOT NULL
),
matched_keys AS (
    SELECT level, id
    FROM filtered
    INTERSECT
    SELECT level, id
    FROM app_logs
    WHERE latency_ms <> 1348
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.level = k.level AND s.id = k.id
)
SELECT
    level,
    latency_ms,
    is_warn,
    service_group,
    attr_key_filled
FROM verified
