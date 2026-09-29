SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id ASC) AS rn FROM web_events) ranked WHERE rn = 1;
