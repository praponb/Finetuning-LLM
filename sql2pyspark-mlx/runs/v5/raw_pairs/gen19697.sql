WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, v
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v,
        EXPLODE(coupon_codes) AS value
    FROM flattened
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM lines
    INTERSECT ALL
    SELECT customer_id, id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        attr_key,
        v,
        UPPER(attr_key) AS attr_key_upper,
        CONCAT(attr_key, ' ', value) AS attr_key_value,
        COALESCE(value, 'OTHER') AS value_clean
    FROM kept_rows
    WHERE created_at > DATE '2025-03-02'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        attr_key,
        v,
        attr_key_upper,
        value_clean,
        CAST(created_at AS DATE) AS created_at_day
    FROM filtered
    WHERE created_at >= TIMESTAMP '2025-03-01 10:00:00'
      AND attr_key IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.country,
        b.segment
    FROM cleaned s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    attr_key,
    value_clean,
    created_at_day,
    segment
FROM with_dims
ORDER BY attr_key DESC NULLS LAST, segment DESC;
