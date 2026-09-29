select customer_id, end_date, monthly_fee, last_day(end_date) as end_date_month_end from subscriptions where status = 'PAUSED';
