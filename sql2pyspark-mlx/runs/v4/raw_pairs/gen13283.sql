SELECT end_date FROM subscriptions EXCEPT SELECT order_date FROM orders WHERE order_date < '2023-07-01';
