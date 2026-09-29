SELECT ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee, SUM(monthly_fee) total_monthly_fee FROM subscriptions WHERE plan = 'enterprise' ORDER BY stddev_monthly_fee DESC
