SELECT monthly_fee, ADD_MONTHS(end_date, 12) AS end_date_plus_months, SUBSTRING(status, 1, 2) AS status_prefix FROM subscriptions WHERE plan IN ('basic', 'standard', 'premium')
