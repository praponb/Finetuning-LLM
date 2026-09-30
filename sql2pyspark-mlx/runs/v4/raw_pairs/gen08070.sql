WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, value
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM exploded
    INTERSECT
    SELECT id
    FROM carts
    WHERE customer_id IN (15, 21, 25)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TO_DATE(created_at) AS created_at_date
    FROM kept_rows
    WHERE created_at >= DATE '2025-03-02'
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN key IN ('gift', 'weight_kg') THEN 'Tier 1' ELSE 'Other' END AS key_group
    FROM base
    WHERE key IS NOT NULL
)
-- Multi-level aggregation
SELECT
    key,
    SIZE(COLLECT_SET(key)) AS num_keys,
    COUNT(*) FILTER (WHERE key = 'gift') AS gift_rows,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    SIZE(COLLECT_SET(key)) AS num_keys_2
FROM base_carts
GROUP BY key
ORDER BY key DESC NULLS LAST;
