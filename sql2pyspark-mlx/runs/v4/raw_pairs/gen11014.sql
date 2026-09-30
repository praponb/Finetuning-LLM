SELECT CASE WHEN amount >= 1410 THEN 'high' WHEN amount >= 310 THEN 'mid' ELSE 'low' END AS amount_bucket, HOUR(paid_at) AS paid_hour, COUNT(*) AS payment_count
FROM payments
GROUP BY 1, 2
