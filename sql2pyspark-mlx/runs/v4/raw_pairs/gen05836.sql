SELECT id, paid_at, method, amount, TO_DATE(paid_at) AS paid_at_date FROM payments WHERE TO_DATE(paid_at) >= '2025-01-01' OR HOUR(paid_at) BETWEEN 7 AND 14
