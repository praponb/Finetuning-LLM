WITH Ranked AS (SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rn FROM web_events) SELECT * FROM Ranked WHERE rn <= 3 ORDER BY page
