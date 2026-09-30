SELECT id, order_id, status, LOWER(status) AS status_lower, CONCAT(method, ' - ', status) AS method_status FROM payments ORDER BY status_lower
