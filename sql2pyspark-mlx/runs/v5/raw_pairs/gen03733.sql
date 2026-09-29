SELECT status, COUNT(*) AS cnt, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM accounts GROUP BY 1 HAVING COUNT(*) >= 3 ORDER BY 3;
