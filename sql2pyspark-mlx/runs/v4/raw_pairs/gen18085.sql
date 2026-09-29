SELECT amount FROM orders WHERE channel <> 'store' EXCEPT SELECT monthly_fee FROM subscriptions;
