SELECT id, started_at, browser, country FROM sessions WHERE HOUR(started_at) BETWEEN 7 AND 14
