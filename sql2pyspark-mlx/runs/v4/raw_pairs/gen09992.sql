SELECT COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count FROM transactions ORDER BY 7_eleven_count;
