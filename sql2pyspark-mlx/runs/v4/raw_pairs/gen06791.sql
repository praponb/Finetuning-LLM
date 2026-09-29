SELECT
    status,
    end_month,
    first_start_date,
    standard_count
FROM (
    SELECT
        status,
        DATE_TRUNC('MONTH', end_date) AS end_month,
        MIN(start_date) AS first_start_date,
        COUNT(CASE WHEN plan = 'standard' THEN 1 END) AS standard_count
    FROM subscriptions
    GROUP BY status, DATE_TRUNC('MONTH', end_date)
) AS x
WHERE standard_count >= 9
