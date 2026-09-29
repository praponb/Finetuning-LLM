SELECT status, ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking FROM accounts GROUP BY status ORDER BY status
