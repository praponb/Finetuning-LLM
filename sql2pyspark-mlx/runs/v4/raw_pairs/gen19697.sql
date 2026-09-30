SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time) AS rnk FROM web_events) AS t WHERE rnk <= 3
