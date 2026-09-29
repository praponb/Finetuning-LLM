SELECT *, SUBSTRING(status, 1, 2) status_prefix, ROUND(monthly_fee, 1) monthly_fee_rounded FROM subscriptions WHERE YEAR(start_date) >= 2024 ORDER BY status_prefix;
