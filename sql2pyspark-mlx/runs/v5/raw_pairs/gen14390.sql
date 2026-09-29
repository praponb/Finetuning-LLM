SELECT AVG(amount) AS avg_amount, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
ORDER BY 2 DESC, avg_amount DESC;
