SELECT order_id, status, DATE_TRUNC('WEEK', paid_at) AS paid_at_period
FROM payments;
