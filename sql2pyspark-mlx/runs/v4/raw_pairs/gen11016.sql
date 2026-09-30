WITH Src AS (
    SELECT *, SUBSTRING(status, 1, 4) status_prefix
    FROM subscriptions
    WHERE customer_id IS NULL
),
start_month_summary AS (
    SELECT DATE_FORMAT(start_date, 'yyyy-MM') start_month, status, MIN(end_date) first_end_date
    FROM Src
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), status
)
SELECT *
FROM start_month_summary
ORDER BY status ASC, 3 DESC;
