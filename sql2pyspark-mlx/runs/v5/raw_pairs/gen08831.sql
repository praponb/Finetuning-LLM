SELECT plan, status, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date FROM subscriptions WHERE NOT (monthly_fee > 60 AND status = 'ACTIVE')
