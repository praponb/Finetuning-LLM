SELECT order_id, method, paid_at, CONCAT_WS(',', status, method) AS status_method_key FROM payments ORDER BY method DESC;
