SELECT is_flagged, 7_eleven_count
FROM (
    SELECT is_flagged, COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 11 AND 15
    GROUP BY is_flagged
) AS t
WHERE 7_eleven_count >= 4
ORDER BY 7_eleven_count;
