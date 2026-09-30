WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date NULLS LAST, id) AS rank_in_group
    FROM subscriptions
    QUALIFY rank_in_group = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM deduped e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL 7 DAYS
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.country,
        d.signup_date
    FROM matched a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, c.email
    FROM joined a
    LEFT JOIN customers c
        ON a.customer_id = c.id
)
-- Grouped metrics
SELECT
    CASE WHEN monthly_fee >= 214 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    DATE_TRUNC('QUARTER', end_date) AS quarter_start,
    SUM(subscription_monthly_fee) AS total_subscription_monthly_fee,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN subscription_plan = 'standard' THEN 1 END), 0), 2) AS monthly_fee_per_standard,
    ROUND(SUM(subscription_id) / NULLIF(COUNT(CASE WHEN subscription_plan = 'basic' THEN 1 END), 0), 2) AS subscription_id_per_basic,
    COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_rows,
    COUNT_IF(plan = 'basic') AS basic_events,
    APPROX_PERCENTILE(monthly_fee, 0.95) AS monthly_fee_p95
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN monthly_fee >= 214 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', end_date)), (CASE WHEN monthly_fee >= 214 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING COUNT_IF(plan = 'basic') < 1
ORDER BY subscription_id_per_basic DESC
