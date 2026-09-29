SELECT status, ROUND((100 * SUM(CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_promptpay
FROM payments
WHERE TO_DATE(paid_at) < '2025-01-01' AND method = 'cash'
GROUP BY status
ORDER BY status DESC
