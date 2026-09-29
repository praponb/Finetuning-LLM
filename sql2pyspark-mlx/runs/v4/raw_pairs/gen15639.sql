SELECT QUARTER(order_date) AS order_quarter, MAX(order_date) AS latest_order_date, MIN(shipping_fee) AS min_shipping_fee
FROM orders
GROUP BY QUARTER(order_date);
