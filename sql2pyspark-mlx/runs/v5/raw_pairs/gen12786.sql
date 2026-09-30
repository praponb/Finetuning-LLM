WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, v
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id ASC, attr_key ASC) AS rn
        FROM lines
    ) x
    WHERE rn = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v,
        rn,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        COALESCE(attr_key, 'DIRECT') AS attr_key_clean
    FROM ranked
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 16:00:00')
      AND attr_key IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v,
        rn,
        created_at_label,
        attr_key_clean,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'NONE') AS attr_key_clean_2
    FROM cleaned
    WHERE attr_key IN ('gift', 'utm_source')
      AND customer_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    customer_id,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT(*) FILTER (WHERE attr_key = 'utm_source') AS utm_source_rows,
    COUNT(CASE WHEN attr_key = 'device' THEN 1 END) AS device_count
FROM valid
GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('DAY', created_at)), (customer_id))
ORDER BY utm_source_rows DESC NULLS FIRST;
