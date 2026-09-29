SELECT
    a.*,
    o.discount,
    o.order_date
FROM shipments a
LEFT JOIN orders o
    ON a.order_id = o.id;
