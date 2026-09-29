SELECT
    p.channel,
    a.status AS payment_status,
    COUNT(*) AS cnt,
    MAX(a.amount) AS max_amount
FROM payments a
INNER JOIN orders p ON a.order_id = p.id
GROUP BY p.channel, a.status;
