WITH exploded AS (SELECT id, coupon_codes, key, value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) x AS key, value) SELECT id, key FROM exploded ORDER BY key DESC;
