WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE start_date >= DATE '2023-06-16'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', start_date) AS week_start,
    CASE WHEN days_start_date_to_end_date >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_start_date_to_end_date_tier,
    SUM(days_start_date_to_end_date) FILTER (WHERE plan = 'standard') AS standard_total_days_start_date_to_end_date,
    ROUND(SUM(days_start_date_to_end_date) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS days_start_date_to_end_date_per_basic,
    AVG(days_start_date_to_end_date) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_days_start_date_to_end_date
FROM cleaned
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', start_date), CASE WHEN days_start_date_to_end_date >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('WEEK', start_date)), ())
HAVING enterprise_avg_days_start_date_to_end_date >= 0
ORDER BY enterprise_avg_days_start_date_to_end_date
