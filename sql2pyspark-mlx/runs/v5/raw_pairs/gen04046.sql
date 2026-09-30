SELECT id, end_date, COALESCE(monthly_fee, 0) monthly_fee_filled
FROM subscriptions;
