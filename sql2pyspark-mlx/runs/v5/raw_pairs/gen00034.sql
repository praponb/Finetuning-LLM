SELECT we.duration_sec, we.event_time, s.customer_id
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
WHERE TO_DATE(we.event_time) >= '2023-09-01' AND s.country IN ('TH', 'UK', 'SG');
