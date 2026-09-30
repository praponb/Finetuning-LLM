WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        status_code,
        attr_key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM lines
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        attr_key,
        v,
        has_fault,
        DENSE_RANK() OVER w_running AS status_code_dense_rank,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_rank
    FROM unpacked
    WHERE attr_key IN ('version', 'region', 'trace')
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY status_code DESC)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE host IN ('h3', 'h1', 'h2')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    status_code,
    attr_key,
    v,
    has_fault,
    CASE WHEN attr_key RLIKE '^(version|region)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS attr_key_zone
FROM verified
ORDER BY attr_key_zone DESC NULLS LAST, has_fault
