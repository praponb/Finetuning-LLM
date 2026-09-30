WITH valid_accounts AS (
    SELECT *, LENGTH(status) AS status_length
    FROM accounts
    WHERE account_type NOT IN ('SAVINGS', 'LOAN') AND (opened_date < '2024-09-12' OR status IS NULL)
),
agg AS (
    SELECT
        status,
        YEAR(opened_date) AS opened_year,
        COUNT(DISTINCT status_length) AS unique_statuss,
        MAX(balance) - MIN(balance) AS balance_range
    FROM valid_accounts
    GROUP BY status, YEAR(opened_date)
)
SELECT *
FROM agg
WHERE unique_statuss > 3
ORDER BY unique_statuss ASC
