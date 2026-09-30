SELECT id, channel, order_date FROM orders WHERE status != 'COMPLETED' EXCEPT SELECT id, channel, order_date FROM orders WHERE shipping_fee >= 13
