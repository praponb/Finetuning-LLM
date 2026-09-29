WITH joined AS (
    -- Attach orders attributes
    SELECT s.*, b.order_date
    FROM payments s
    INNER JOIN orders b
        ON s.order_id = b.id
    WHERE b.customer_id = 1
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.status,
    s.order_date,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_unit_price
FROM joined s;
