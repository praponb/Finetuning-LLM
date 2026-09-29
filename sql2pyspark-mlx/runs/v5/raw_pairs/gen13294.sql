SELECT ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(DISTINCT amount) AS unique_methods FROM payments WHERE amount <= 1360 OR status = 'REFUNDED' ORDER BY unique_methods DESC;
