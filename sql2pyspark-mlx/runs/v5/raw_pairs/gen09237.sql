WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SIZE(coupon_codes) AS coupon_codes_count,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS is_spring10
    FROM exploded
    WHERE attr_key IN ('weight_kg', 'device', 'gift')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v,
        coupon_codes_count,
        is_spring10
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY coupon_codes_count DESC NULLS FIRST, id, attr_key) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY attr_key, customer_id ORDER BY coupon_codes_count DESC) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.coupon_codes,
    s.attr_key,
    s.coupon_codes_count,
    s.is_spring10,
    s.latest_rank,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM deduped s
ORDER BY coupon_codes_count, attr_key
