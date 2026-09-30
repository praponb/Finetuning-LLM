WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, v
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        v,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM exploded
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN value = 'SPRING10' THEN 1 ELSE 0 END is_spring10,
        UPPER(value) value_upper
    FROM lines
    WHERE value IN ('WELCOME5', 'SPRING10')
)
SELECT
    id,
    value,
    is_spring10,
    value_upper
FROM valid
ORDER BY id DESC NULLS FIRST
