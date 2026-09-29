SELECT monthly_fee FROM subscriptions UNION ALL SELECT shipping_fee AS monthly_fee FROM orders;
