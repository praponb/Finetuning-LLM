SELECT status, SUM(shipping_fee) AS total_shipping_fee FROM orders WHERE amount > 860 AND MONTH(order_date) IN (1, 3, 11) GROUP BY status HAVING SUM(shipping_fee) < 90;
