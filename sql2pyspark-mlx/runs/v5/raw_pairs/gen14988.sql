WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        service,
        status_code,
        attr_key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, attr_key ORDER BY status_code, id, attr_key) AS rn
        FROM lines
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        service,
        attr_key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code, id, attr_key) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(attr_key) AS attr_key_upper
    FROM latest
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'region'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        service,
        attr_key,
        SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
        SIZE(COLLECT_SET(service)) AS num_services,
        COUNT_IF(attr_key = 'trace') AS trace_events,
        COUNT(DISTINCT service) AS distinct_services
    FROM cleaned
    GROUP BY 1, 2
)
SELECT
    service,
    attr_key,
    attr_key_list,
    num_services,
    trace_events,
    distinct_services
FROM summary
ORDER BY num_services DESC;
