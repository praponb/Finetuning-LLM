SELECT CASE WHEN amount >= 1750 THEN 'high' WHEN amount >= 460 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
GROUP BY amount_bucket
HAVING AVG(amount) > 850
