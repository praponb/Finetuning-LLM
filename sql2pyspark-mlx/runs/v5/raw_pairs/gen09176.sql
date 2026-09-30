SELECT *, UPPER(status) AS status_upper, CAST(paid_at AS DATE) AS paid_at_day
FROM payments
WHERE amount <= 1250 AND HOUR(paid_at) BETWEEN 6 AND 11
ORDER BY status_upper, id
LIMIT 10
