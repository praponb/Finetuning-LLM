SELECT *, DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label
FROM sessions
ORDER BY started_at_label
