WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        LEAD(monthly_fee, 1) OVER (ORDER BY monthly_fee ASC, id ASC) AS next_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share
    FROM subscriptions
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id ASC) AS seq_no,
        AVG(next_monthly_fee) OVER w_grp AS next_monthly_fee_avg_5_prior,
        MAX(monthly_fee) OVER w_running AS status_max_monthly_fee
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY plan ORDER BY start_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_running AS (PARTITION BY status)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY next_monthly_fee_avg_5_prior DESC NULLS LAST, id ASC) = 1
),
metrics_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        status,
        next_monthly_fee_avg_5_prior,
        status_max_monthly_fee,
        FIRST_VALUE(next_monthly_fee_avg_5_prior, TRUE) OVER (ORDER BY status_max_monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_monthly_fee_avg_5_prior,
        CUME_DIST() OVER (PARTITION BY status, plan ORDER BY seq_no DESC) AS seq_no_cume_dist,
        AVG(next_monthly_fee_avg_5_prior) OVER (PARTITION BY plan ORDER BY start_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_monthly_fee_avg_5_prior_avg_3
    FROM latest
    WHERE next_monthly_fee BETWEEN 120 AND 180
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            plan,
            first_known_next_monthly_fee_avg_5_prior
        FROM metrics_subscriptions
    ) src
    PIVOT (
        SUM(first_known_next_monthly_fee_avg_5_prior)
        FOR plan IN ('enterprise' AS enterprise, 'basic' AS basic)
    )
)
SELECT
    status,
    enterprise,
    basic,
    CASE WHEN status RLIKE '^(ACTIVE|EXPIRED)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS status_zone
FROM wide;
