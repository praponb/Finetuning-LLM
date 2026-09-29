SELECT s.*, o.amount
FROM order_items s
LEFT JOIN orders o
    ON s.order_id = o.id;
