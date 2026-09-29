SELECT customer_id, channel FROM orders WHERE MONTH(order_date) IN (2, 4, 10) UNION SELECT customer_id, channel FROM orders WHERE shipping_fee >= 26;
