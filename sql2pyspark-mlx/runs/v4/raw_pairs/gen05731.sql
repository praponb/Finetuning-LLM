SELECT method, SUM(CASE WHEN method = 'cash' THEN amount ELSE 0 END) AS cash_amount, MIN(amount) AS min_amount
FROM payments
GROUP BY method
HAVING COUNT(amount) < 3
ORDER BY min_amount DESC
