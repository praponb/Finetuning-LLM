SELECT CASE WHEN sessions.browser IN ('Safari', 'Edge') THEN 'group_a' ELSE 'other' END AS browser_group, AVG(web_events.duration_sec) AS avg_duration_sec, SUM(web_events.duration_sec) AS total_duration_sec
FROM web_events
RIGHT JOIN sessions ON web_events.session_id = sessions.id
GROUP BY CASE WHEN sessions.browser IN ('Safari', 'Edge') THEN 'group_a' ELSE 'other' END
ORDER BY browser_group, avg_duration_sec DESC;
