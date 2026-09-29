SELECT DISTINCT browser, country FROM sessions WHERE HOUR(started_at) BETWEEN 12 AND 17
