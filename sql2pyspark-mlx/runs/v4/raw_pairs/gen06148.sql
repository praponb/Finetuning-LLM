SELECT id, amount, paid_at, method, MONTH(paid_at) paid_at_month FROM payments WHERE paid_at < '2024-09-01' ORDER BY method;
