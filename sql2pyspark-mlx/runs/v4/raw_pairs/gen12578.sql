WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE opened_date BETWEEN '2023-04-01' AND '2023-09-16'
),
accounts_stats AS (
    SELECT status, ROUND(STDDEV(balance), 2) AS stddev_balance
    FROM filtered_accounts
    GROUP BY status
)
SELECT status, stddev_balance
FROM accounts_stats
WHERE stddev_balance > 22
ORDER BY stddev_balance DESC;
