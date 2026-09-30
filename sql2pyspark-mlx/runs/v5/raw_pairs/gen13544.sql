SELECT
    id,
    customer_id,
    coupon_codes,
    pos + 1 AS line_no,
    line.product_id
FROM carts
LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, line;
