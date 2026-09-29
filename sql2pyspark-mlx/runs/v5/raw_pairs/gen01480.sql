SELECT plan, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee FROM subscriptions WHERE plan = 'enterprise' GROUP BY plan ORDER BY stddev_monthly_fee DESC
