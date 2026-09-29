SELECT CASE WHEN amount >= 1570 THEN 'high' WHEN amount >= 510 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE order_id = 30 AND amount BETWEEN 440 AND 1240
GROUP BY CASE WHEN amount >= 1570 THEN 'high' WHEN amount >= 510 THEN 'mid' ELSE 'low' END
