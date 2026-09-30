WITH exploded AS (SELECT id, customer_id, k, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) e AS k, attr_value) SELECT id, customer_id, k FROM exploded ORDER BY k DESC NULLS FIRST, id;
