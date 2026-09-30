WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC NULLS FIRST, id, attr_key) AS rank_in_group
        FROM flattened
    ) x
    WHERE rank_in_group = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY service ORDER BY status_code DESC) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(attr_key, ' ', service) AS attr_key_service
    FROM deduped
    WHERE attr_key IS NOT NULL
),
latest_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, service ORDER BY latency_ms DESC, id, attr_key) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
deduped_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service, attr_key ORDER BY latest_rank DESC, id, attr_key) = 1
)
SELECT
    id,
    status_code,
    attr_key,
    attr_value,
    CASE WHEN attr_key RLIKE '^(region|version)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS attr_key_zone
FROM deduped_app_logs
ORDER BY status_code DESC
