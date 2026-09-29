SELECT
    d.channel,
    a.status AS payment_status,
    COUNT(*) AS rows_total,
    ROUND(AVG(a.amount), 2) AS avg_amount
FROM payments a
JOIN orders d ON a.order_id = d.id
GROUP BY d.channel, a.status
ORDER BY channel DESC NULLS LAST
