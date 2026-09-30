WITH lines AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
base AS (
    SELECT
        *,
        CASE WHEN key IN ('region', 'trace') THEN 'Priority' ELSE 'Other' END AS key_group,
        COALESCE(key, 'NONE') AS key_clean
    FROM lines
    WHERE key IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        attr_value,
        key_group,
        key_clean,
        ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms DESC, id ASC, key ASC) AS seq_no,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms ASC) AS latency_ms_rank
    FROM base
)
SELECT
    id,
    service,
    status_code,
    key,
    attr_value,
    key_group,
    key_clean,
    seq_no,
    latency_ms_rank,
    CASE WHEN service RLIKE '^(auth|search)$' THEN 'EXTENDED' WHEN service RLIKE '^(billing|checkout)$' THEN 'GROUP_A' ELSE 'CORE' END AS service_zone
FROM with_windows;
