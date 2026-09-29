WITH kept_keys AS (
    SELECT account_type
    FROM accounts
    EXCEPT
    SELECT account_type
    FROM accounts
    WHERE status != 'OPEN'
),
kept_rows AS (
    SELECT s.*
    FROM accounts s
    INNER JOIN kept_keys k ON s.account_type = k.account_type
)
SELECT
    CASE WHEN balance >= 79000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    MAX(balance) AS max_balance,
    COUNT_IF(account_type = 'SAVINGS') AS savings_events,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN balance >= 79000 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(status)) DESC) AS num_status_percent_rank
FROM kept_rows
GROUP BY balance_tier, year_start
HAVING COUNT_IF(account_type = 'SAVINGS') < 1
ORDER BY max_balance NULLS LAST;
