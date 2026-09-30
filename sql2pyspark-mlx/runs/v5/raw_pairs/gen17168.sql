WITH lines AS (SELECT id, customer_id, key, attr_value FROM carts LATERAL VIEW EXPLODE(attributes) li AS key, attr_value) SELECT id, customer_id, key, attr_value FROM lines ORDER BY customer_id DESC;
