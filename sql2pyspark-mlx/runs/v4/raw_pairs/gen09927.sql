SELECT id, end_date, status, start_date, DATE_ADD(start_date, 37) AS start_date_plus_37d FROM subscriptions ORDER BY end_date, id LIMIT 50;
