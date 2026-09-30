SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    o.amount,
    p.launch_date,
    CAST(o.discount AS INT) AS discount_int
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id;
