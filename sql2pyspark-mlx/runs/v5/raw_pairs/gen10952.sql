select id, start_date, lower(status) as status_lower from subscriptions where monthly_fee <= 170;
