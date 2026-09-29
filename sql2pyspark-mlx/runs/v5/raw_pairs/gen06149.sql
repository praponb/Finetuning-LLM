WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(attr_key) AS attr_key_lower
    FROM lines
    WHERE attr_key IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        COALESCE(attr_key, 'NONE') AS resolved_attr_key
    FROM cleaned
    WHERE attr_key IN ('utm_source', 'device', 'weight_kg')
      AND created_at > TO_DATE('2025-03-02')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('month', created_at) AS created_at_period,
        COALESCE(attr_key, 'Unknown') AS attr_key_filled,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key_2
    FROM base
    WHERE created_at >= DATE '2025-03-01'
      AND attr_key IS DISTINCT FROM 'utm_source'
)
-- Aggregate with conditional metrics
SELECT
    attr_key,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows_2,
    COUNT(*) FILTER (WHERE attr_key = 'device') AS device_rows,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    MIN(created_at) AS first_seen
FROM filtered
GROUP BY attr_key
ORDER BY num_attr_keys DESC NULLS FIRST
