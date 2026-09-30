SELECT merchant, ROUND((100 * SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shell FROM transactions GROUP BY merchant
