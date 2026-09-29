SELECT page, SUM(duration_sec) AS total_duration_sec, SUM(SUM(duration_sec)) OVER (ORDER BY page) AS cumulative_duration_sec FROM web_events GROUP BY page;
