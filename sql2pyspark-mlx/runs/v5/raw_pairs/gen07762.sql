SELECT unit_price FROM order_items EXCEPT SELECT monthly_fee FROM subscriptions ORDER BY unit_price
