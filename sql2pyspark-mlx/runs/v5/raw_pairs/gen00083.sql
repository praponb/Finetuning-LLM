SELECT method, ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success, SUM(amount) AS total_amount
FROM payments
WHERE status = 'REFUNDED' AND method NOT IN ('card', 'cash', 'bank_transfer')
GROUP BY 1
ORDER BY method
