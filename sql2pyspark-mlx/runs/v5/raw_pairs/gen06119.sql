WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee NULLS LAST, id) AS rank_in_group
        FROM subscriptions
    ) t
    WHERE rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN monthly_fee >= 241 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS monthly_fee_per_enterprise,
    COUNT(*) AS event_count
FROM deduped
GROUP BY 1
ORDER BY event_count DESC
