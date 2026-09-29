select id, start_date, customer_id from subscriptions where start_date = (select min(start_date) from subscriptions where end_date >= '2024-04-01');
