SELECT id, customer_id, end_date, plan, CONCAT(status, '/', plan) AS status_plan FROM subscriptions ORDER BY plan ASC
