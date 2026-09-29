WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS line
)
SELECT
    id,
    coupon_codes,
    product_id,
    qty
FROM lines;
