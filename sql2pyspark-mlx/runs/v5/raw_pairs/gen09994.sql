SELECT location, cnt FROM (SELECT location, COUNT(*) AS cnt FROM departments GROUP BY location) AS s WHERE cnt > 4 ORDER BY cnt DESC, location LIMIT 10;
