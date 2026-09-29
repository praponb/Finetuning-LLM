SELECT id, EXPLODE(coupon_codes) AS element
FROM carts;
