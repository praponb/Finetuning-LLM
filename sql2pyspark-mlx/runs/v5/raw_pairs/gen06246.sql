SELECT id, created_at, EXPLODE(coupon_codes) AS code FROM carts
