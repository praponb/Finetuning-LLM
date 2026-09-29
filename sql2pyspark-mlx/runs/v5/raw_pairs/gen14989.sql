SELECT d.country, e.plan, COUNT(*) AS num_rows, MAX(e.monthly_fee) AS max_monthly_fee FROM subscriptions e LEFT JOIN customers d ON e.customer_id = d.id GROUP BY d.country, e.plan
