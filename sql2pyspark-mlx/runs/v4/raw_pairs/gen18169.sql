SELECT id, end_date, status FROM subscriptions WHERE monthly_fee < 220 AND (plan != 'standard' OR monthly_fee IS NOT NULL);
