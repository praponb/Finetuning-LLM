WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE account_type IN ('SAVINGS', 'CREDIT') AND (status = 'OPEN' OR account_type IN ('CHECKING', 'CREDIT', 'LOAN'))
),
Summary AS (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(balance) AS total_balance, ROUND((100 * SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_savings
    FROM valid_accounts
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT opened_month, pct_savings
FROM Summary
ORDER BY pct_savings DESC
