WITH exploded AS (
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
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        attr_value,
        EXPLODE(coupon_codes) AS element
    FROM exploded
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, p.city
    FROM exploded_carts a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.created_at,
        src.key,
        src.attr_value,
        src.element,
        src.city,
        p.segment,
        p.age,
        p.city AS customer_city
    FROM with_dims src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.is_active = FALSE
)
SELECT
    s.customer_id,
    s.created_at,
    s.key,
    s.attr_value,
    s.element,
    s.segment,
    s.age,
    s.customer_city,
    (SELECT MIN(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM with_dims_carts s
