SELECT monthly_fee FROM subscriptions WHERE start_date >= '2024-09-22' EXCEPT SELECT cost FROM products
