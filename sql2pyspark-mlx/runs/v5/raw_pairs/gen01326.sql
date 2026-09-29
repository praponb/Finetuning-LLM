WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS elem
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        qty,
        price,
        EXPLODE(coupon_codes) AS element
    FROM exploded
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(price / qty, 2) AS price_per_qty,
        COALESCE(element, 'OTHER') AS resolved_element
    FROM flattened
    WHERE element IN ('WELCOME5', 'VIP', 'SPRING10')
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        qty,
        price,
        element,
        price_per_qty,
        ROUND(price_per_qty, 0) AS price_per_qty_rounded
    FROM base
    WHERE element IS DISTINCT FROM 'WELCOME5'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        price,
        element,
        price_per_qty,
        price_per_qty_rounded,
        FLOOR(price_per_qty) AS price_per_qty_whole
    FROM base_carts
    WHERE element IS NOT NULL
      AND price_per_qty_rounded > 61
)
SELECT
    element,
    price_per_qty,
    price_per_qty_rounded,
    CASE WHEN element RLIKE '^(VIP|BUNDLE)$' THEN 'CORE' WHEN element RLIKE '^(SPRING10|WELCOME5)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS element_zone
FROM filtered
