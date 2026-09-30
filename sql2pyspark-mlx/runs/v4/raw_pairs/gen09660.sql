WITH src AS (
    SELECT *
    FROM accounts
    WHERE balance > 73000 AND (customer_id IS NOT NULL OR account_type = 'LOAN')
),
summary AS (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, MIN(balance) AS min_balance, COUNT(*) AS row_count
    FROM src
    GROUP BY DATE_TRUNC('MONTH', opened_date)
)
SELECT *
FROM summary
WHERE row_count < 6
ORDER BY opened_month
