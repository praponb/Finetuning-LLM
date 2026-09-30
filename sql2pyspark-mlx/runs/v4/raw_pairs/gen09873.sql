SELECT
    id,
    customer_id,
    created_at,
    k,
    v
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) x AS k, v;
