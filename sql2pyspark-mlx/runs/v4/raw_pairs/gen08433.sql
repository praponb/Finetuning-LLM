SELECT * FROM sessions WHERE HOUR(started_at) BETWEEN 6 AND 10 ORDER BY browser, id DESC
