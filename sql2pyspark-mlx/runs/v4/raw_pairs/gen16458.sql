WITH cleaned AS (
    SELECT *, TRIM(plan) AS clean_plan
    FROM subscriptions
    WHERE monthly_fee BETWEEN 110 AND 150
),
metrics AS (
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_3,
        AVG(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior
    FROM cleaned
),
with_windows AS (
    SELECT
        *,
        RANK() OVER (ORDER BY monthly_fee NULLS LAST) AS monthly_fee_rank,
        LEAD(monthly_fee_avg_3, 1) OVER (PARTITION BY customer_id, status ORDER BY monthly_fee_avg_2_prior, id) AS next_monthly_fee_avg_3,
        LAG(monthly_fee, 2, 0) OVER w_grp AS prev_monthly_fee
    FROM metrics
    WINDOW w_grp AS (PARTITION BY plan ORDER BY end_date, id)
),
metrics_subscriptions AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY prev_monthly_fee DESC NULLS LAST) AS prev_monthly_fee_percent_rank,
        SUM(next_monthly_fee_avg_3) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_monthly_fee_avg_3
    FROM with_windows
)
SELECT
    customer_id,
    ROUND(AVG(monthly_fee_rank), 2) AS avg_monthly_fee_rank,
    ROUND(AVG(monthly_fee_rank), 2) AS avg_monthly_fee_rank_2,
    COUNT(*) FILTER (WHERE plan = 'standard') AS standard_rows,
    SIZE(COLLECT_SET(status)) AS num_status,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM metrics_subscriptions
GROUP BY customer_id
HAVING num_plans > 1
