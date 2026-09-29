WITH RankedWebEvents AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS rnk FROM web_events) SELECT id, page, event_time FROM RankedWebEvents WHERE rnk <= 3
