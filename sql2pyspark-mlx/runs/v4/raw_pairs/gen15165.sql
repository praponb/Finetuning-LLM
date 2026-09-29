SELECT
    oi.id,
    oi.product_id,
    o.status,
    o.order_date,
    s.opened_date,
    s.city
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN stores s ON s.id = o.store_id
ORDER BY id DESC, order_date;
