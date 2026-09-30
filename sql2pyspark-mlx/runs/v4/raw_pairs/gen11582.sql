WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(attr_key)) AS attr_key_title
    FROM lines
    WHERE attr_key = 'utm_source'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS is_spring10,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM filtered
    WHERE attr_key IS NOT NULL
      AND ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    attr_key,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
FROM reconciled
GROUP BY attr_key
