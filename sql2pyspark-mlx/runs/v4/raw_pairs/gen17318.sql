SELECT ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner, COUNT(*) AS cnt FROM orders ORDER BY cnt DESC;
