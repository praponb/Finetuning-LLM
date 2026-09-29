SELECT MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions WHERE monthly_fee > 110 OR customer_id IN (2, 3, 24, 35);
