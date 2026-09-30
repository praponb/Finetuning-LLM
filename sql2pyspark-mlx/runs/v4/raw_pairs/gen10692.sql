WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, attr_value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        key,
        EXPLODE(coupon_codes) AS value
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', value, key) value_key_key
    FROM flattened
    WHERE value IS NOT NULL
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(key) key_upper
    FROM filtered
    WHERE key IS NOT NULL
)
-- Multi-level aggregation
SELECT
    key,
    COUNT(DISTINCT key) AS distinct_keys,
    COUNT(*) FILTER (WHERE key = 'weight_kg') AS weight_kg_rows,
    COUNT(CASE WHEN value = 'SPRING10' THEN 1 END) AS spring10_count
FROM filtered_carts
GROUP BY key;
