SELECT CASE WHEN amount >= 3800 THEN 'high' WHEN amount >= 800 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE amount < 1900
GROUP BY 1
ORDER BY avg_amount DESC;
