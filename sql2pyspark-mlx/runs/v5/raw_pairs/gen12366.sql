select id, start_date, date_sub(start_date, 77) as start_date_minus_77d from subscriptions where month(end_date) in (1, 6, 12) and monthly_fee < 180 and plan in ('basic', 'enterprise')
