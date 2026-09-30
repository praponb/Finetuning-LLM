SELECT oi.id, oi.unit_price, o.amount FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id WHERE o.amount BETWEEN 770 AND 1150
