SELECT account_type, total_count FROM (SELECT account_type, COUNT(*) AS total_count FROM accounts GROUP BY account_type) AS s WHERE total_count > 5 ORDER BY account_type DESC, total_count
