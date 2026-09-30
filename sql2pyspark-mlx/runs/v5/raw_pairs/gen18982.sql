SELECT CASE WHEN amount >= 1700 THEN 'high' WHEN amount >= 460 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) AS card_amount
FROM payments
GROUP BY amount_bucket
ORDER BY amount_bucket, card_amount DESC;
