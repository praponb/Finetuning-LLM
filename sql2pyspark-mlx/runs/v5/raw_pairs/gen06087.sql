SELECT
    s.delivered_date,
    s.order_id,
    o.amount,
    c.country,
    LEAST(o.shipping_fee, o.discount) AS least_shipping_fee_discount
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.channel = 'partner' OR c.age <> 38
ORDER BY country DESC;
