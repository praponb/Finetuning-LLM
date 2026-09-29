WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(start_date, 1) AS start_date_plus_months
    FROM subscriptions
    WHERE monthly_fee < 90
      AND customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, b.age
    FROM valid e
    LEFT JOIN customers b
        ON e.customer_id <=> b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        AVG(age) OVER w_running AS age_avg_6,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        NTILE(10) OVER (ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY age) AS age_dense_rank,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee
    FROM joined
    WHERE customer_id IN (16, 20, 26, 28)
    WINDOW w_running AS (PARTITION BY status, plan ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY age_dense_rank NULLS LAST, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    SUM(CASE WHEN plan = 'premium' THEN monthly_fee_bucket ELSE 0 END) AS premium_monthly_fee_bucket,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    MIN(monthly_fee_bucket) AS min_monthly_fee_bucket,
    APPROX_PERCENTILE(monthly_fee_bucket, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_bucket_quartiles,
    COUNT(*) AS cnt,
    MAX(age_dense_rank) FILTER (WHERE status = 'PAUSED') AS paused_max_age_dense_rank
FROM deduped
GROUP BY year_start
ORDER BY min_monthly_fee_bucket DESC NULLS LAST
