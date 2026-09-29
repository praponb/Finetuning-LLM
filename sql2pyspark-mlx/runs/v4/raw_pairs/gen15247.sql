SELECT
    oi.id,
    oi.unit_price,
    o.store_id,
    o.channel,
    s.region,
    o.amount - o.discount amount_minus_discount
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
RIGHT JOIN stores s ON o.store_id = s.id
WHERE oi.product_id = 5;
