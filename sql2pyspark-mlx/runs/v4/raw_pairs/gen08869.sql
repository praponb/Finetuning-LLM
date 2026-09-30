SELECT id, start_date FROM subscriptions WHERE start_date = (SELECT MAX(start_date) FROM subscriptions);
