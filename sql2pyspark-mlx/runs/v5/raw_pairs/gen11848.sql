SELECT
    oi.order_id,
    oi.unit_price,
    o.amount,
    c.is_active,
    c.segment
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
WHERE LENGTH(c.email) >= 9
ORDER BY c.segment ASC;
