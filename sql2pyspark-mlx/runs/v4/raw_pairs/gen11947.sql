SELECT paid_at, status FROM payments WHERE TO_DATE(paid_at) < '2024-01-28';
