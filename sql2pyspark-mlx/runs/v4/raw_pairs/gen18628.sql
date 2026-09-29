SELECT id, store_id, order_date, shipping_fee, LEAD(shipping_fee, 1) OVER (PARTITION BY store_id ORDER BY order_date, id) AS next_shipping_fee FROM orders ORDER BY store_id DESC
