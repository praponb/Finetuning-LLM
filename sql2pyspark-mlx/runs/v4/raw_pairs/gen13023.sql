SELECT id, order_id, COUNT(*) OVER (PARTITION BY order_id) AS order_id_count FROM payments ORDER BY id DESC;
