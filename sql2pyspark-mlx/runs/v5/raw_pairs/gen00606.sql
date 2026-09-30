SELECT id, status, UPPER(status) status_upper
FROM payments
WHERE HOUR(paid_at) BETWEEN 12 AND 20
ORDER BY id DESC
LIMIT 5;
