SELECT MAX(amount) - MIN(amount) AS amount_range, AVG(discount) AS avg_discount FROM orders ORDER BY amount_range DESC;
