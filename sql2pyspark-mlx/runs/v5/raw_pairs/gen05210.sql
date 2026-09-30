SELECT MONTH(c.signup_date) AS signup_month, MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range, SORT_ARRAY(COLLECT_SET(s.status)) AS status_list
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date)
