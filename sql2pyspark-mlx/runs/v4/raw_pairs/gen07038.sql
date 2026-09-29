SELECT
    status,
    ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash,
    COUNT(DISTINCT amount) AS unique_statuss,
    COUNT(amount) AS amount_count
FROM payments
WHERE order_id IN (11, 19, 22, 40)
GROUP BY status;
