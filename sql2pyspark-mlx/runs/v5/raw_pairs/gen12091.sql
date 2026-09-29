SELECT status, AVG(monthly_fee) AS avg_monthly_fee, COUNT(DISTINCT monthly_fee) AS unique_customers, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list FROM subscriptions GROUP BY status
