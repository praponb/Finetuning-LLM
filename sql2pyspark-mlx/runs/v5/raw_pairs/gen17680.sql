WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        value,
        value_2
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) li AS value_2
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(value_2) AS value_2_upper,
        COALESCE(k, 'OTHER') AS k_filled
    FROM lines_carts
    WHERE value_2 IS NOT NULL
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        k,
        value,
        value_2,
        value_2_upper,
        k_filled,
        CONCAT_WS('-', k, value_2) AS k_value_2_key,
        COALESCE(value_2, 'OTHER') AS value_2_filled
    FROM cleaned
    WHERE value_2 NOT IN ('SPRING10', 'VIP')
)
SELECT
    id,
    k,
    value,
    value_2,
    k_filled,
    value_2_filled
FROM cleaned_carts
