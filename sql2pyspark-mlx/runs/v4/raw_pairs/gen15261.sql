SELECT *, CAST(monthly_fee AS INT) monthly_fee_int, DATE_TRUNC('week', start_date) start_date_period FROM subscriptions WHERE customer_id IS NOT NULL;
