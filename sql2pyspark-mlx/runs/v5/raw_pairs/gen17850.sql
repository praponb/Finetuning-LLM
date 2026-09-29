SELECT *, CONCAT_WS(',', plan, status) AS plan_status_key, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
FROM subscriptions
WHERE monthly_fee <= 130 AND status = 'EXPIRED' AND status = 'PAUSED';
