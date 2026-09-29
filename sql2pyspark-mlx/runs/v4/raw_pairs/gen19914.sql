SELECT id, created_at, k, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) e AS k, attr_value
