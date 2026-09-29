SELECT MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, AVG(monthly_fee) AS avg_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count FROM subscriptions;
