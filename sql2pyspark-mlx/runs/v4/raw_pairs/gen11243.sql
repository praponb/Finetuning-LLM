SELECT SORT_ARRAY(COLLECT_SET(plan)) AS plan_list, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE (end_date < '2023-08-09' AND plan = 'enterprise') OR plan = 'premium'
