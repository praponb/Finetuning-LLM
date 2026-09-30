SELECT order_date FROM orders WHERE amount < 1410 EXCEPT SELECT end_date FROM subscriptions WHERE start_date <= '2025-09-01'
