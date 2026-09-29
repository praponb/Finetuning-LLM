WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan, customer_id), 4) AS monthly_fee_share,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_3_prior
    FROM subscriptions
)
-- Aggregate with conditional metrics
SELECT
    plan,
    SUM(CASE WHEN status = 'CANCELLED' THEN monthly_fee ELSE 0 END) AS cancelled_monthly_fee,
    SUM(monthly_fee_share) AS total_monthly_fee_share,
    COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_rows
FROM metrics
GROUP BY plan
HAVING expired_rows < 1000
ORDER BY expired_rows ASC
