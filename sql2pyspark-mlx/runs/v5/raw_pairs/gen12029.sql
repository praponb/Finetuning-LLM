SELECT status, MAX(amount) - MIN(amount) amount_range
FROM orders
GROUP BY status
ORDER BY status, amount_range DESC;
