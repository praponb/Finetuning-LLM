SELECT status, ROUND((100 * SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paypal
FROM payments
WHERE amount BETWEEN 1080 AND 1220 AND method = 'paypal'
GROUP BY status
HAVING COUNT(*) >= 2
ORDER BY pct_paypal, status
