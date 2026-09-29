SELECT page, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec FROM web_events WHERE NOT (page = '/home' AND duration_sec <= 205) GROUP BY page;
