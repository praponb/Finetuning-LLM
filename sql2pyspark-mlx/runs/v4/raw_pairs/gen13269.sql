SELECT CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'priority' ELSE 'other' END AS status_group, ROUND((100 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_closed
FROM accounts
WHERE customer_id = 25 OR MONTH(opened_date) IN (3, 6, 12)
GROUP BY CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'priority' ELSE 'other' END
HAVING ROUND((100 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 71
ORDER BY pct_closed ASC
