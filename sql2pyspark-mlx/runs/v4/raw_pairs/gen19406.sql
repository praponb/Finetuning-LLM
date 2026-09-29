SELECT id, end_date FROM subscriptions WHERE end_date <= (SELECT MIN(end_date) FROM subscriptions)
