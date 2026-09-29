SELECT status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, COUNT(*) AS total_count FROM subscriptions GROUP BY status;
