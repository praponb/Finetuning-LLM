WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC NULLS LAST, id ASC) AS rank_in_group
    FROM subscriptions
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    customer_id,
    COUNT(DISTINCT plan) AS distinct_plans,
    SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(DISTINCT status) AS distinct_status_2
FROM deduped
GROUP BY customer_id
HAVING SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) > 0
ORDER BY expired_monthly_fee ASC NULLS LAST;
