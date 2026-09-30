SELECT
    id,
    customer_id,
    key,
    attr_value
FROM carts
LATERAL VIEW EXPLODE(attributes) li AS key, attr_value
