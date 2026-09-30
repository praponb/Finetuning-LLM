SELECT id, shipping_fee, DATE_FORMAT(order_date, 'yyyy-MM-dd') AS order_date_label FROM orders ORDER BY shipping_fee DESC
