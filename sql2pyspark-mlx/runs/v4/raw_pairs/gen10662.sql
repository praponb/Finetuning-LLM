WITH exploded AS (SELECT id, customer_id, created_at, k, value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS k, value) SELECT id, k FROM exploded ORDER BY k NULLS FIRST, id
