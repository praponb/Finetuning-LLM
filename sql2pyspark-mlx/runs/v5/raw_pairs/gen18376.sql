WITH valid_accounts AS (
    SELECT *, ROUND(balance * 1.15, 2) AS balance_adjusted
    FROM accounts
    WHERE status NOT IN ('FROZEN', 'CLOSED') OR balance >= 55000
),
opened_month_summary AS (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, MAX(opened_date) AS latest_opened_date, COUNT(DISTINCT balance_adjusted) AS unique_statuss
    FROM valid_accounts
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT opened_month, latest_opened_date, unique_statuss
FROM opened_month_summary
WHERE unique_statuss > 4
ORDER BY opened_month DESC;
