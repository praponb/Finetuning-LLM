WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) = 1
)
-- Multi-level aggregation
SELECT
    status,
    DATE_TRUNC('YEAR', start_date) AS year_start,
    SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM deduped
GROUP BY GROUPING SETS ((status, DATE_TRUNC('YEAR', start_date)), (status), ())
ORDER BY year_start
