SELECT start_date, end_date FROM subscriptions WHERE customer_id IS NOT NULL INTERSECT SELECT start_date, end_date FROM subscriptions WHERE monthly_fee <= 160;
