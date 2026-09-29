WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.name,
        b.credit_limit
    FROM valid s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_whole,
        name,
        credit_limit,
        STDDEV_SAMP(credit_limit) OVER w_ordered AS status_stddev_samp_credit_limit,
        NTILE(3) OVER w_ordered_2 AS monthly_fee_bucket,
        CUME_DIST() OVER (ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_cume_dist,
        LAG(monthly_fee_whole, 1) OVER (ORDER BY credit_limit, id) AS prev_monthly_fee_whole,
        NTILE(10) OVER (PARTITION BY plan ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM joined
    WINDOW w_ordered AS (PARTITION BY status), w_ordered_2 AS (PARTITION BY plan ORDER BY monthly_fee, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_cume_dist DESC NULLS FIRST) AS monthly_fee_cume_dist_dense_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share
    FROM scored
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN monthly_fee_cume_dist >= 0.76 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_cume_dist_tier,
        COUNT_IF(plan = 'enterprise') AS enterprise_events,
        ROUND(SUM(status_stddev_samp_credit_limit) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS status_stddev_samp_credit_limit_per_premium,
        COUNT(*) AS row_count
    FROM with_windows
    GROUP BY CUBE (CASE WHEN monthly_fee_cume_dist >= 0.76 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING status_stddev_samp_credit_limit_per_premium >= 0
)
SELECT
    monthly_fee_cume_dist_tier,
    status_stddev_samp_credit_limit_per_premium,
    row_count
FROM stats
ORDER BY monthly_fee_cume_dist_tier NULLS LAST
