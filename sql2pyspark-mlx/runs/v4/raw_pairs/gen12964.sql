SELECT id, customer_id, created_at, key, v FROM carts LATERAL VIEW EXPLODE(attributes) x AS key, v;
