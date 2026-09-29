WITH recent_accounts AS (
    SELECT *
    FROM accounts
    WHERE opened_date BETWEEN '2025-01-01' AND '2025-02-02'
),
Summary AS (
    SELECT status, MAX(opened_date) AS last_opened_date, COUNT(*) AS cnt
    FROM recent_accounts
    GROUP BY status
)
SELECT *
FROM Summary
ORDER BY last_opened_date, status
LIMIT 10;
