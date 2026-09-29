SELECT paid_at, TO_DATE(paid_at) AS paid_at_date
FROM payments
WHERE status IN ('FAILED', 'REFUNDED') AND amount >= 980
ORDER BY paid_at;
