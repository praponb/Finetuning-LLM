SELECT e.*, d.status AS order_status
FROM shipments e
LEFT JOIN orders d
    ON e.order_id = d.id;
