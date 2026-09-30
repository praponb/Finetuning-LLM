WITH exploded AS (SELECT id, created_at, coupon_codes, INLINE(items) FROM carts) SELECT id, created_at, coupon_codes, qty FROM exploded ORDER BY qty NULLS FIRST;
