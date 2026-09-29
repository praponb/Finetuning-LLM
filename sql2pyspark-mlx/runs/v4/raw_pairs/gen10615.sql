SELECT start_date FROM subscriptions EXCEPT SELECT last_restock FROM inventory WHERE product_id IN (4, 6, 7, 8) ORDER BY start_date
