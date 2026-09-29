SELECT dim.country, dim.browser, COUNT(*) AS rows_total, MIN(s.duration_sec) AS min_duration_sec FROM web_events s INNER JOIN sessions dim ON s.session_id = dim.id GROUP BY dim.country, dim.browser
