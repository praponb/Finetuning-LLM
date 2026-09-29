SELECT id, customer_id, order_date, channel, DATE_ADD(order_date, 1) AS order_date_plus_1d FROM orders WHERE amount >= 1260 ORDER BY order_date_plus_1d, id DESC
