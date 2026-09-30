SELECT
    o.id,
    o.channel,
    COUNT(p.id) payment_count,
    ROUND((100 * SUM(CASE WHEN p.method = 'bank_transfer' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_bank_transfer
FROM orders o
LEFT OUTER JOIN payments p ON o.id = p.order_id
GROUP BY o.id, o.channel
ORDER BY payment_count, id, channel
LIMIT 1
