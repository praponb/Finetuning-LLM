SELECT shipping_fee AS value FROM orders EXCEPT SELECT monthly_fee AS value FROM subscriptions WHERE monthly_fee <= 220;
