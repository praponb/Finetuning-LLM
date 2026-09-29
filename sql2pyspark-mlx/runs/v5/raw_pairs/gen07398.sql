SELECT channel, MAX(order_date) AS latest_order_date FROM orders WHERE shipping_fee > 33 GROUP BY channel;
