SELECT method, DATE_TRUNC('MONTH', paid_at) paid_month, COUNT(*) cnt
FROM payments
GROUP BY method, DATE_TRUNC('MONTH', paid_at)
HAVING COUNT(*) >= 1
ORDER BY 2, cnt DESC
