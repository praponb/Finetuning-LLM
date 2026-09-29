SELECT method, ROUND(AVG(amount), 2) AS avg_amount, ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed
FROM payments
GROUP BY 1
ORDER BY avg_amount DESC, method ASC
LIMIT 1;
