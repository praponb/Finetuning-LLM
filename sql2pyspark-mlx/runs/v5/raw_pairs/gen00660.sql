SELECT
    id,
    customer_id,
    created_at,
    key,
    attr_value
FROM carts
LATERAL VIEW EXPLODE(attributes) t AS key, attr_value;
