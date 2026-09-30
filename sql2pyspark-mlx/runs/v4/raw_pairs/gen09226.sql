SELECT CASE WHEN amount >= 1660 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY CASE WHEN amount >= 1660 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END) AS cumulative_amount
FROM payments
GROUP BY CASE WHEN amount >= 1660 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END
ORDER BY 1 DESC;
