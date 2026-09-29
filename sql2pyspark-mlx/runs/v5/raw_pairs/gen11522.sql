SELECT id, start_date, monthly_fee, ROUND(AVG(monthly_fee) OVER (ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_7 FROM subscriptions
