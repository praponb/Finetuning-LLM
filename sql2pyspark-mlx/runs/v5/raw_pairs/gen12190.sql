SELECT *, CONCAT(plan, ' ', status) AS plan_status FROM subscriptions WHERE NOT (monthly_fee >= 200 AND MONTH(start_date) IN (1, 4, 7)) ORDER BY end_date ASC
