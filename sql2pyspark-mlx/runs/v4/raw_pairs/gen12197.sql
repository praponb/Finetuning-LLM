WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level IN ('DEBUG', 'INFO') THEN 'Core' ELSE 'Other' END AS level_group
    FROM exploded
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND attr_key IS NOT NULL
      AND level = 'INFO'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(host) AS host_upper,
        LOWER(level) AS level_lower,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM filtered
    WHERE log_ts > TO_DATE('2025-06-01')
      AND attr_key IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(log_ts) AS log_ts_hour,
        UPPER(attr_key) AS attr_key_upper
    FROM base
    WHERE log_ts > TIMESTAMP '2025-06-01 07:00:00'
      AND level IN ('DEBUG', 'ERROR', 'INFO')
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        service,
        host,
        attr_key,
        v,
        level_group,
        attr_key_filled,
        log_ts_hour,
        attr_key_upper,
        CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END AS is_error
    FROM valid
    WHERE log_ts >= DATE '2025-06-01'
      AND attr_key IS NOT NULL
      AND attr_key = 'version'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT host, service
    FROM valid_app_logs
    INTERSECT
    SELECT host, service
    FROM app_logs
    WHERE TO_DATE(log_ts) < '2023-03-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_app_logs s
    JOIN remaining_keys k ON s.host = k.host AND s.service = k.service
),
enriched AS (
    -- Ranking and gap features
    SELECT
        service,
        host,
        attr_key,
        v,
        level_group,
        attr_key_filled,
        log_ts_hour,
        attr_key_upper,
        is_error,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY log_ts_hour DESC NULLS FIRST) AS log_ts_hour_cume_dist,
        MAX(log_ts_hour) OVER (PARTITION BY service) AS service_max_log_ts_hour
    FROM verified
)
SELECT
    v,
    level_group,
    attr_key_filled,
    attr_key_upper,
    is_error,
    log_ts_hour_cume_dist,
    service_max_log_ts_hour
FROM enriched
ORDER BY log_ts_hour_cume_dist DESC;
