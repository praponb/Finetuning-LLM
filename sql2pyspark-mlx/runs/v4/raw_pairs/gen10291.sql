SELECT id, end_date, DAY(end_date) AS end_date_day
FROM subscriptions
WHERE plan != 'basic' AND status != 'PAUSED'
ORDER BY end_date DESC
