SELECT status FROM orders EXCEPT SELECT status FROM subscriptions ORDER BY status;
