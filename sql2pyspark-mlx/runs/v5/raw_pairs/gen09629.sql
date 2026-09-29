SELECT is_flagged, SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END) AS 7_eleven_count FROM transactions GROUP BY is_flagged HAVING 7_eleven_count > 25;
