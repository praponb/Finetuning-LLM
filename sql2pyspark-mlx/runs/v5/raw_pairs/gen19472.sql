WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
),
flattened_carts AS (
    SELECT
        id,
        created_at,
        attr_key,
        attr_value,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) x AS element
),
filtered AS (
    SELECT
        *,
        TO_DATE(created_at) created_at_date,
        COALESCE(attr_key, 'DIRECT') attr_key_filled
    FROM flattened_carts
    WHERE created_at > DATE '2025-03-01'
      AND attr_key IS NOT NULL
),
base AS (
    SELECT
        created_at,
        attr_key,
        attr_value,
        created_at_date,
        attr_key_filled,
        DAY(created_at) created_at_day
    FROM filtered
    WHERE attr_key IS NOT NULL
)
SELECT
    attr_key,
    attr_value,
    created_at_date,
    attr_key_filled,
    created_at_day
FROM base
ORDER BY attr_key DESC
