SELECT c.segment, ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
FULL OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment
ORDER BY stddev_duration_sec DESC, segment
LIMIT 50;
