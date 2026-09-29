SELECT order_id, method FROM payments WHERE status IN ('REFUNDED', 'FAILED') EXCEPT SELECT order_id, method FROM payments WHERE TO_DATE(paid_at) < '2025-02-13' ORDER BY order_id;
