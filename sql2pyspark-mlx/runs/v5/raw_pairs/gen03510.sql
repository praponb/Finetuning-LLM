SELECT
    o.id,
    o.channel,
    o.status,
    o.order_date
FROM orders o
WHERE o.amount BETWEEN 530 AND 780 AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id = 18)
ORDER BY id DESC;
