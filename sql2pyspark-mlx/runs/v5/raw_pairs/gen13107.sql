SELECT YEAR(paid_at) AS paid_year, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE paid_at < '2024-01-01'
GROUP BY YEAR(paid_at)
HAVING ROUND(AVG(amount), 2) >= 65;
