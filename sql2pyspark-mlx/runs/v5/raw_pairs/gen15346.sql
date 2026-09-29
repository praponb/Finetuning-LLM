WITH enriched AS (
    SELECT
        *,
        AVG(monthly_fee) OVER w_recent AS monthly_fee_avg_3_prior,
        NTILE(10) OVER w_recent_2 AS monthly_fee_bucket
    FROM subscriptions
    WINDOW w_recent AS (ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_recent_2 AS (ORDER BY monthly_fee DESC, id)
),
filtered AS (
    SELECT *, DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM enriched
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        monthly_fee_avg_3_prior,
        monthly_fee_bucket,
        end_date_dayofweek,
        SUM(monthly_fee_avg_3_prior) OVER w_grp AS running_sum_monthly_fee_avg_3_prior,
        AVG(monthly_fee_avg_3_prior) OVER w_grp AS running_avg_monthly_fee_avg_3_prior,
        MIN(monthly_fee_avg_3_prior) OVER w_grp AS running_min_monthly_fee_avg_3_prior,
        MAX(monthly_fee_avg_3_prior) OVER w_grp AS running_max_monthly_fee_avg_3_prior
    FROM filtered
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_max_monthly_fee_avg_3_prior NULLS LAST, id) = 1
)
SELECT
    id,
    start_date,
    end_date,
    status,
    monthly_fee_avg_3_prior,
    monthly_fee_bucket,
    end_date_dayofweek,
    running_avg_monthly_fee_avg_3_prior,
    running_min_monthly_fee_avg_3_prior
FROM ranked
ORDER BY id DESC NULLS LAST
LIMIT 10
