SELECT status, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions WHERE start_date > '2023-12-01' GROUP BY status ORDER BY max_monthly_fee ASC;
