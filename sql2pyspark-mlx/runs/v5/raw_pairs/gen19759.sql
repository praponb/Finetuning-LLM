WITH filtered_subscriptions AS (
    SELECT *, LENGTH(status) AS status_length
    FROM subscriptions
    WHERE plan = 'enterprise' AND (plan = 'enterprise' OR status = 'PAUSED')
),
Summary AS (
    SELECT CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort, MIN(end_date) AS min_end_date, SUM(status_length) AS total_status_length
    FROM filtered_subscriptions
    GROUP BY CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT end_cohort, min_end_date, total_status_length
FROM Summary
ORDER BY total_status_length
