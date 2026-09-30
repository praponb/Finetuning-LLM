SELECT id, created_at, k, attr_value FROM carts LATERAL VIEW EXPLODE(attributes) li AS k, attr_value
