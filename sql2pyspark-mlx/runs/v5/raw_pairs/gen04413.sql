SELECT method, YEAR(paid_at) paid_year, MAX(amount) max_amount
FROM payments
GROUP BY method, YEAR(paid_at)
ORDER BY max_amount DESC;
