SELECT
    id,
    attr_key,
    attr_value
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, attr_value;
