WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
matched_keys AS (
    -- Set difference on keys
    SELECT service
    FROM exploded
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE status_code <= 263
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN matched_keys k ON s.service = k.service
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(key, 'N/A') AS key_filled
    FROM reconciled
    WHERE key NOT IN ('version', 'trace')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE host IN ('h1', 'h3')
),
reconciled_app_logs AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id
),
grouped AS (
    -- Grouped metrics
    SELECT
        service,
        COUNT_IF(service = 'search') AS search_events,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        COUNT(*) FILTER (WHERE key = 'version') AS version_rows
    FROM reconciled_app_logs
    GROUP BY 1
)
SELECT service, key_list
FROM grouped
