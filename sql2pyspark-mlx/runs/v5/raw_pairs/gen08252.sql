SELECT id, end_date, ROUND(monthly_fee, 2) AS monthly_fee_rounded FROM subscriptions WHERE monthly_fee IS NOT NULL
