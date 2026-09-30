SELECT *
FROM payments
WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS') AND (status NOT IN ('FAILED', 'REFUNDED') OR order_id = 26);
