SELECT status, DATE_TRUNC('MONTH', end_date) AS end_month, MIN(start_date) AS min_start_date FROM subscriptions GROUP BY status, end_month ORDER BY status ASC
