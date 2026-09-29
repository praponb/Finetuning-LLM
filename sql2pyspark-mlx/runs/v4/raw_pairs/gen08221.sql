SELECT
    s.*,
    o.order_date,
    o.status,
    o.channel
FROM order_items s
INNER JOIN orders o
    ON s.order_id = o.id
