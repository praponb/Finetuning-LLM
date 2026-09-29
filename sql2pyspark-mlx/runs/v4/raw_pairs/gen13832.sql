WITH exploded AS (SELECT id, tag FROM carts LATERAL VIEW EXPLODE(coupon_codes) e AS tag) SELECT id FROM exploded ORDER BY id;
