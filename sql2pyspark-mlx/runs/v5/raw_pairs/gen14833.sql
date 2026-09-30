SELECT id, end_date, monthly_fee, DATE_ADD(end_date, 68) AS end_date_plus_68d FROM subscriptions WHERE status IN ('EXPIRED', 'CANCELLED')
