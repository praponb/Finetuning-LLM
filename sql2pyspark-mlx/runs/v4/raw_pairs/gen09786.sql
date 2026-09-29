WITH lines AS (SELECT id, customer_id, tag FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS tag) SELECT customer_id, tag FROM lines ORDER BY tag NULLS LAST;
