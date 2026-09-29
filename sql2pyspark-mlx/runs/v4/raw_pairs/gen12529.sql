SELECT status, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
WHERE MONTH(start_date) IN (1, 2, 7) AND status = 'ACTIVE' AND status = 'ACTIVE'
GROUP BY 1
HAVING MIN(monthly_fee) >= 180
