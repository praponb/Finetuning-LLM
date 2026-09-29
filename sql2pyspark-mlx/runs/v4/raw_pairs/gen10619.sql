SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, status, COUNT(*) AS row_count
FROM payments
WHERE order_id IS NULL
GROUP BY DATE_TRUNC('MONTH', paid_at), status
ORDER BY status DESC, 1;
