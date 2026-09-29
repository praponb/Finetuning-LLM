SELECT id, EXPLODE(coupon_codes) AS tag FROM carts;
