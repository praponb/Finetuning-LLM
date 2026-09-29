SELECT status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, MAX(start_date) AS last_start_date FROM subscriptions WHERE plan = 'enterprise' GROUP BY status;
