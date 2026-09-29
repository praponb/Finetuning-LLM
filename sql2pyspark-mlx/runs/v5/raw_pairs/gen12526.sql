SELECT method, status, COUNT(*) AS row_count
FROM payments
GROUP BY method, status
ORDER BY method DESC, row_count
