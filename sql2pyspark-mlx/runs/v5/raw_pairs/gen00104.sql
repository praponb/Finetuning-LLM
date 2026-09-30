SELECT id, start_date, end_date, status, DATE_SUB(start_date, 48) AS start_date_minus_48d FROM subscriptions WHERE start_date >= '2025-10-01' OR customer_id IN (4, 12, 16);
