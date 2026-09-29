SELECT
    o.id,
    o.channel,
    COUNT(p.id) AS payment_count,
    SUM(CASE WHEN p.status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count,
    AVG(p.amount) AS avg_amount
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE o.customer_id IS NOT NULL
GROUP BY o.id, o.channel
HAVING COUNT(p.id) > 5
ORDER BY avg_amount;
