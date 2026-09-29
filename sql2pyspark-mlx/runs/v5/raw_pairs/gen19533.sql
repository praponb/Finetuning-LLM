SELECT id, status, order_date, shipping_fee, LAG(shipping_fee, 1) OVER (PARTITION BY status ORDER BY order_date, id) AS prev_shipping_fee FROM orders ORDER BY prev_shipping_fee
