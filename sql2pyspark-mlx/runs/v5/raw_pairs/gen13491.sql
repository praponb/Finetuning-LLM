SELECT QUARTER(end_date) AS end_quarter, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee FROM subscriptions GROUP BY 1 ORDER BY end_quarter ASC, avg_monthly_fee DESC;
