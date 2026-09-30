SELECT status, DATE_FORMAT(paid_at, 'yyyy-MM') paid_at_label
FROM payments
WHERE method = 'card'
ORDER BY status DESC;
