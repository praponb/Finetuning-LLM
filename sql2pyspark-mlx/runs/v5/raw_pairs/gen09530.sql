SELECT
    o.id,
    o.discount,
    o.shipping_fee,
    o.status,
    c.credit_limit,
    UPPER(o.channel) AS channel_upper
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE c.credit_limit <= 10400
ORDER BY channel_upper, 4
