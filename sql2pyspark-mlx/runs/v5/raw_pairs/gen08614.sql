SELECT DATE_TRUNC('MONTH', end_date) AS end_month, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY end_month
