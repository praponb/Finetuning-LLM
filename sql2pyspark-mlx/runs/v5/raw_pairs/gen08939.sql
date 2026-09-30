WITH account_summary AS (
    SELECT account_id, COUNT(*) AS cnt, COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.status,
    as.cnt,
    as.shell_count
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id
WHERE as.cnt >= 7
ORDER BY account_type DESC, id
LIMIT 10;
