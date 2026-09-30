SELECT id, country, browser FROM sessions WHERE TO_DATE(started_at) >= '2023-04-04' EXCEPT SELECT id, country, browser FROM sessions WHERE device IN ('mobile', 'desktop');
