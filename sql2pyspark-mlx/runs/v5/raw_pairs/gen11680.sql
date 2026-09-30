SELECT start_date FROM subscriptions INTERSECT SELECT last_restock FROM inventory ORDER BY start_date
