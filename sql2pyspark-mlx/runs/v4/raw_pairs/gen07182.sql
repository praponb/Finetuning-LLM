SELECT start_date FROM subscriptions EXCEPT SELECT sale_date AS start_date FROM store_sales ORDER BY start_date;
