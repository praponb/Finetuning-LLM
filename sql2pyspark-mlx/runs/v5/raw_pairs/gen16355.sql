SELECT id, status, start_date, monthly_fee, LAG(monthly_fee, 1, 0) OVER (PARTITION BY status ORDER BY start_date, id) AS prev_monthly_fee FROM subscriptions WHERE monthly_fee BETWEEN 80 AND 240
