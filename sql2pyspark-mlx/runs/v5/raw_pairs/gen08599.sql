SELECT DATE_TRUNC('MONTH', event_time) AS event_month, SUM(duration_sec) AS total_duration_sec, RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk
FROM web_events
WHERE event_type <> 'purchase'
GROUP BY DATE_TRUNC('MONTH', event_time);
