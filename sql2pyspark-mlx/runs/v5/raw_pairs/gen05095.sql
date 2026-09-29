WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
),
flattened AS (
    SELECT
        id,
        customer_id,
        key,
        code
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
kept_keys AS (
    SELECT customer_id, id
    FROM flattened
    INTERSECT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id IS NULL
),
reconciled AS (
    SELECT s.*
    FROM flattened s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
filtered AS (
    SELECT *, LENGTH(key) AS key_length
    FROM reconciled
    WHERE code IN ('WELCOME5', 'VIP')
)
SELECT
    s.id,
    s.customer_id,
    s.key,
    s.code,
    CASE WHEN key RLIKE '^(weight_kg|device)$' THEN 'EXTENDED' ELSE 'CORE' END AS key_zone,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM filtered s;
