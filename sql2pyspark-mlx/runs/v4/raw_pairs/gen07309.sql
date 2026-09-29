WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled,
        LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-11'
      AND customer_id IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN monthly_fee >= 250 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count,
    COUNT_IF(status = 'ACTIVE') AS active_events,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(monthly_fee) FILTER (WHERE plan = 'premium') AS premium_max_monthly_fee,
    ROUND(SUM(monthly_fee_filled) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS monthly_fee_filled_per_enterprise
FROM valid
GROUP BY monthly_fee_tier
HAVING active_events > 1;
