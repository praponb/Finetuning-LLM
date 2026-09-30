SELECT id, status, start_date, monthly_fee, LAG(monthly_fee, 1, 0) OVER (PARTITION BY status ORDER BY start_date, id) AS prev_monthly_fee FROM subscriptions ORDER BY prev_monthly_fee DESC, id DESC;
