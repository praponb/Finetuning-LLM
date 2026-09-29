WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        host,
        attr_key,
        attr_value
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY log_ts DESC, id, attr_key) AS dedup_rank
        FROM exploded
    ) t
    WHERE dedup_rank = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(attr_key, 1, 4) AS attr_key_prefix,
        CASE WHEN attr_key = 'trace' THEN 1 ELSE 0 END AS is_trace,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM deduped
    WHERE attr_key IS NOT NULL
      AND attr_key IS NOT DISTINCT FROM 'version'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        attr_key,
        attr_value,
        attr_key_prefix,
        attr_key_filled,
        CONCAT_WS('-', attr_key, host) AS attr_key_host_key
    FROM valid
    WHERE is_trace <= 42
),
daily AS (
    -- Multi-level aggregation
    SELECT
        attr_key,
        host,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(CASE WHEN attr_key = 'region' THEN 1 END) AS region_count,
        COUNT_IF(host = 'h4') AS h4_events
    FROM base
    GROUP BY attr_key, host
)
SELECT
    attr_key,
    host,
    distinct_ids,
    region_count,
    h4_events,
    CASE WHEN host RLIKE '^(h2|h1)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS host_zone
FROM daily
ORDER BY host_zone DESC;
