SELECT id, monthly_fee, start_date, CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS is_active FROM subscriptions ORDER BY 3 ASC;
