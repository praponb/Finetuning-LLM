SELECT monthly_fee FROM subscriptions UNION ALL SELECT revenue AS monthly_fee FROM store_sales ORDER BY monthly_fee DESC;
