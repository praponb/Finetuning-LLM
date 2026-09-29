WITH matched_keys AS (
    -- Set intersection on keys
    SELECT plan
    FROM subscriptions
    INTERSECT ALL
    SELECT plan
    FROM subscriptions
    WHERE monthly_fee > 80
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    INNER JOIN matched_keys k ON s.plan = k.plan
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    CASE WHEN monthly_fee >= 261 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY COUNT(*) DESC) AS row_count_dense_rank
FROM kept_rows
GROUP BY activity_month, monthly_fee_tier
ORDER BY activity_month DESC NULLS FIRST;
