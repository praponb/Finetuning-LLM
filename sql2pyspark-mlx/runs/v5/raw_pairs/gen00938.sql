SELECT
    p.amount,
    o.discount,
    o.order_date,
    c.age,
    c.country,
    FLOOR(o.amount) amount_whole
FROM payments p
RIGHT OUTER JOIN orders o ON p.order_id = o.id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.shipping_fee BETWEEN 24 AND 30
