SELECT * FROM (SELECT page, COUNT(CASE WHEN page = '/search' THEN 1 END) search_count FROM web_events GROUP BY page) s WHERE search_count < 8
