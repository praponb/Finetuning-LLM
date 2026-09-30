SELECT id, status, monthly_fee, DATE_FORMAT(start_date, 'yyyy-MM-dd') start_date_label FROM subscriptions WHERE status = 'ACTIVE';
