WITH src AS (
    SELECT *
    FROM accounts
    WHERE status IN ('OPEN', 'CLOSED', 'FROZEN')
),
summary AS (
    SELECT account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_open
    FROM src
    GROUP BY account_type
)
SELECT account_type, account_type_values, pct_open
FROM summary
ORDER BY pct_open DESC, account_type_values ASC, account_type ASC
LIMIT 20;
