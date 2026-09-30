WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
flattened AS (
    SELECT
        id,
        customer_id,
        attr_key,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
joined AS (
    SELECT
        src.*,
        d.credit_limit,
        d.email
    FROM flattened src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
kept_keys AS (
    SELECT id
    FROM joined
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
verified AS (
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    SELECT
        id,
        customer_id,
        attr_key,
        code,
        credit_limit,
        email,
        LOWER(attr_key) AS attr_key_lower
    FROM verified
    WHERE code IS NOT NULL
),
joined_carts AS (
    SELECT
        a.*,
        d.email AS customer_email,
        d.segment
    FROM cleaned a
    INNER JOIN customers d
        ON a.customer_id = d.id
)
SELECT *
FROM (
    SELECT
        customer_id,
        attr_key,
        credit_limit
    FROM joined_carts
) src
PIVOT (
    MAX(credit_limit) AS amount,
    COUNT(1) AS n
    FOR attr_key IN ('device' AS device, 'gift' AS gift, 'weight_kg' AS weight_kg)
);
