WITH with_dims AS (
    -- Attach customers attributes
    SELECT src.*, b.name
    FROM subscriptions src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS rank_in_group
        FROM with_dims
    ) t
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM latest
    INTERSECT ALL
    SELECT id, plan
    FROM subscriptions
    WHERE plan = 'basic'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    AVG(monthly_fee) AS avg_monthly_fee,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    PERCENTILE_APPROX(monthly_fee, 0.95) AS monthly_fee_p95
FROM reconciled
GROUP BY 1
ORDER BY avg_monthly_fee DESC;
