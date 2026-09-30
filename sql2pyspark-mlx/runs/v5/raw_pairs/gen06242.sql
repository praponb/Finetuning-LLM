SELECT id, method, CONCAT_WS(',', method, status) AS method_status_key, HOUR(paid_at) AS paid_at_hour FROM payments;
