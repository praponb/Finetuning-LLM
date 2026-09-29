SELECT id, paid_at, method, status, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at FROM payments WHERE method NOT IN ('promptpay', 'cash') AND status NOT IN ('REFUNDED', 'FAILED')
