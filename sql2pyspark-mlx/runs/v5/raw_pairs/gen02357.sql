WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id IN (2, 24, 32, 39)
),
accounts_stats AS (
    SELECT customer_id, MAX(balance) - MIN(balance) balance_range
    FROM valid_accounts
    GROUP BY customer_id
)
SELECT *
FROM accounts_stats
ORDER BY customer_id DESC
LIMIT 1;
