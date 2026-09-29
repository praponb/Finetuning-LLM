SELECT status, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions GROUP BY 1 HAVING SUM(monthly_fee) > 1310 ORDER BY status;
