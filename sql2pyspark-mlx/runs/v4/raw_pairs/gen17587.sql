SELECT status, pct_open, account_count
FROM (
    SELECT status, ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_open, COUNT(*) AS account_count
    FROM accounts
    GROUP BY status
) AS t
WHERE pct_open < 23
ORDER BY account_count DESC;
