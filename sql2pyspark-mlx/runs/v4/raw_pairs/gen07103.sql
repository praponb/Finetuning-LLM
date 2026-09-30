WITH flattened AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
)
SELECT
    id,
    coupon_codes,
    attr_key,
    attr_value
FROM flattened;
