SELECT id, code
FROM carts
LATERAL VIEW EXPLODE(coupon_codes) t AS code;
