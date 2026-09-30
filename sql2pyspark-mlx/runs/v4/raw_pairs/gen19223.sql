SELECT web_events.duration_sec, sessions.started_at, COALESCE(sessions.browser, 'unassigned') AS browser_filled
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.country = 'DE' OR sessions.country IN ('SG', 'DE');
