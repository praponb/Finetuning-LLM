WITH cleaned AS (
    SELECT *, ROUND(monthly_fee, 0) AS monthly_fee_rounded
    FROM subscriptions
    WHERE start_date > DATE '2023-07-01'
),
with_windows AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        NTILE(10) OVER (ORDER BY monthly_fee_rounded, id) AS monthly_fee_rounded_bucket,
        LAG(monthly_fee, 2) OVER (PARTITION BY customer_id, status ORDER BY start_date, id) AS prev_monthly_fee
    FROM cleaned
),
valid AS (
    SELECT
        id,
        plan,
        monthly_fee,
        monthly_fee_rounded_bucket,
        prev_monthly_fee,
        YEAR(end_date) AS end_date_year,
        TRIM(plan) AS clean_plan
    FROM with_windows
    WHERE start_date >= TO_DATE('2023-03-30')
      AND customer_id IS NOT NULL
),
metrics AS (
    SELECT
        id,
        monthly_fee,
        COUNT(prev_monthly_fee) OVER w_hist AS last7_count_prev_monthly_fee,
        SUM(prev_monthly_fee) OVER w_hist AS last7_sum_prev_monthly_fee,
        MIN(prev_monthly_fee) OVER w_hist AS last7_min_prev_monthly_fee
    FROM valid
    WINDOW w_hist AS (PARTITION BY plan ORDER BY end_date_year, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
metrics_subscriptions AS (
    SELECT
        id,
        monthly_fee,
        last7_count_prev_monthly_fee,
        last7_min_prev_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY last7_sum_prev_monthly_fee, id) AS seq_no,
        LAG(monthly_fee, 2) OVER (ORDER BY last7_min_prev_monthly_fee, id) AS prev_monthly_fee,
        AVG(last7_count_prev_monthly_fee) OVER (ORDER BY last7_min_prev_monthly_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last7_count_prev_monthly_fee_avg_6_prior,
        FIRST_VALUE(last7_sum_prev_monthly_fee, TRUE) OVER (ORDER BY last7_count_prev_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last7_sum_prev_monthly_fee,
        SUM(last7_sum_prev_monthly_fee) OVER (ORDER BY last7_count_prev_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last7_sum_prev_monthly_fee
    FROM metrics
)
SELECT
    seq_no,
    ROUND(AVG(running_last7_sum_prev_monthly_fee), 2) AS avg_running_last7_sum_prev_monthly_fee,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics_subscriptions
GROUP BY seq_no
ORDER BY distinct_ids;
