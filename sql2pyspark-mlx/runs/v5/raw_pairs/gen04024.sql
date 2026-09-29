SELECT id, created_at, code FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code;
