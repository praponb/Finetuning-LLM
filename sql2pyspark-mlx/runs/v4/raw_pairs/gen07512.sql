SELECT *, DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label FROM sessions WHERE device NOT IN ('tablet', 'mobile') ORDER BY id
