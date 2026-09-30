SELECT DATE_TRUNC('MONTH', start_date) AS start_month, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY start_month
HAVING ROUND(STDDEV(monthly_fee), 2) < 36;
