SELECT * FROM (SELECT plan, status, MIN(monthly_fee) min_monthly_fee, AVG(monthly_fee) avg_monthly_fee FROM subscriptions GROUP BY plan, status) s WHERE min_monthly_fee >= 190 ORDER BY plan DESC, 2
