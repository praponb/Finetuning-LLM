SELECT
    status,
    CASE WHEN amount >= 1360 THEN 'high' WHEN amount >= 470 THEN 'mid' ELSE 'low' END AS amount_bucket,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed
FROM payments
WHERE order_id = 18
GROUP BY status, CASE WHEN amount >= 1360 THEN 'high' WHEN amount >= 470 THEN 'mid' ELSE 'low' END
HAVING pct_failed > 25
ORDER BY pct_failed DESC;
