WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(attr_key, 'N/A') AS attr_key_filled,
        CASE WHEN attr_key IN ('weight_kg', 'utm_source') THEN 'Core' ELSE 'Other' END AS attr_key_group
    FROM exploded
    WHERE created_at >= TO_DATE('2025-03-02')
      AND attr_key IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) < '2025-01-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(attr_key, 'unassigned') AS attr_key_filled_2,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM verified
    WHERE attr_key NOT IN ('weight_kg', 'device')
)
SELECT
    id,
    created_at,
    attr_key,
    attr_key_filled,
    attr_key_group,
    attr_key_filled_2,
    resolved_attr_key
FROM base;
