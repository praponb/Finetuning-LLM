SELECT MAX(monthly_fee) AS max_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count FROM subscriptions WHERE customer_id IN (2, 13, 33) ORDER BY max_monthly_fee DESC, monthly_fee_count DESC
