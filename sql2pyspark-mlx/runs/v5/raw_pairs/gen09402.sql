WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id IS NULL
),
status_summary AS (
    SELECT status, MIN(opened_date) min_opened_date
    FROM filtered_accounts
    GROUP BY status
)
SELECT *
FROM status_summary
ORDER BY 1 DESC, min_opened_date;
