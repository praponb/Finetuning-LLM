SELECT shipping_fee FROM orders WHERE channel = 'store' EXCEPT SELECT monthly_fee FROM subscriptions ORDER BY shipping_fee;
