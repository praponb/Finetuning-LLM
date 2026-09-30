SELECT id, customer_id, created_at, attr_key, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, attr_value;
