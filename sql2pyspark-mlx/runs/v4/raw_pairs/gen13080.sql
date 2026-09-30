SELECT id, amount, UPPER(status) AS status_upper
FROM payments
WHERE amount >= 550
ORDER BY status_upper;
