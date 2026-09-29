SELECT id, order_id, paid_at, status FROM payments WHERE paid_at >= '2025-03-02' AND TO_DATE(paid_at) >= '2023-05-17' ORDER BY 2 DESC
