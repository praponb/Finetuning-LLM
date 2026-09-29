WITH exploded AS (
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
deduped AS (
    SELECT
        id,
        host,
        attr_key,
        attr_value
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY latency_ms DESC, id, attr_key) AS dedup_rank
        FROM exploded
    ) ranked
    WHERE dedup_rank = 1
),
valid AS (
    SELECT
        *,
        UPPER(host) AS host_upper,
        CONCAT_WS('-', host, attr_key) AS host_attr_key_key,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM deduped
    WHERE host NOT IN ('h3', 'h4')
),
base AS (
    SELECT *, CONCAT(host, ' ', attr_key) AS host_attr_key
    FROM valid
    WHERE attr_key IS NOT NULL
)
SELECT
    id,
    attr_key,
    attr_value,
    host_upper,
    host_attr_key_key,
    attr_key_filled,
    host_attr_key
FROM base
ORDER BY attr_key;
