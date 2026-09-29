SELECT method, status FROM payments WHERE paid_at < '2023-08-10' UNION SELECT method, status FROM payments WHERE status IN ('SUCCESS', 'FAILED', 'REFUNDED') ORDER BY method DESC;
