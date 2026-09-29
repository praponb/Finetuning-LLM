SELECT s.*
FROM orders s
WHERE EXISTS (
        SELECT 1
        FROM order_items c
        WHERE c.order_id = s.id
          AND c.product_id IN (14, 35)
    )
