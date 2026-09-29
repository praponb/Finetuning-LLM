WITH base AS (
    SELECT *
    FROM accounts
    WHERE balance <= 16000 AND account_type = 'CREDIT'
),
AccountsStats AS (
    SELECT status, COUNT(*) AS total_count
    FROM base
    GROUP BY status
)
SELECT status, total_count
FROM AccountsStats
WHERE total_count >= 9
ORDER BY total_count DESC, status
LIMIT 50
