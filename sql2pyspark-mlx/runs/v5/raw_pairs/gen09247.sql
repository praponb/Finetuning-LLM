SELECT oi.order_id, oi.unit_price, oi.product_id, o.amount, o.customer_id, o.shipping_fee FROM order_items oi LEFT OUTER JOIN orders o ON oi.order_id = o.id WHERE oi.product_id IN (25, 39)
