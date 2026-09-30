SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, MAX(shipping_fee) AS max_shipping_fee, AVG(discount) AS avg_discount FROM orders GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
