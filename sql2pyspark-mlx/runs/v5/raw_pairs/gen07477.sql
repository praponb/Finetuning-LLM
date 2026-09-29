WITH exploded AS (SELECT id, customer_id, EXPLODE_OUTER(coupon_codes) AS value FROM carts) SELECT customer_id, value FROM exploded;
