SELECT id, start_date, RANK() OVER (ORDER BY start_date) AS start_date_rank FROM subscriptions;
