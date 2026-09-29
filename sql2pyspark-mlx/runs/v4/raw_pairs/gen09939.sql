SELECT status, MAX(order_date) AS latest_order_date, ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee FROM orders GROUP BY status
