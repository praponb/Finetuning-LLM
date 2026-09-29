WITH valid AS (
    SELECT *, ADD_MONTHS(end_date, 6) end_date_plus_months
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-02-16')
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) monthly_fee_share,
        LAG(monthly_fee, 2, 0) OVER (ORDER BY end_date, id) prev_monthly_fee,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status, plan ORDER BY end_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY status, plan ORDER BY end_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee,
        NTILE(10) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) monthly_fee_bucket
    FROM valid
),
with_dims AS (
    SELECT s.*, b.city
    FROM enriched s
    JOIN customers b
        ON s.customer_id = b.id
),
with_windows AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        end_date_plus_months,
        prev_monthly_fee,
        first_known_monthly_fee,
        monthly_fee_bucket,
        ROUND(monthly_fee_bucket / SUM(monthly_fee_bucket) OVER (PARTITION BY status), 4) monthly_fee_bucket_share,
        MIN(monthly_fee_share) OVER (PARTITION BY customer_id) customer_id_min_monthly_fee_share
    FROM with_dims
    WHERE MONTH(end_date) IN (5, 8, 9)
),
grouped AS (
    SELECT
        DATE_TRUNC('YEAR', end_date_plus_months) AS year_start,
        CASE WHEN prev_monthly_fee >= 200 THEN 'LARGE' ELSE 'SMALL' END AS prev_monthly_fee_tier,
        COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count,
        SUM(monthly_fee_bucket) FILTER (WHERE status = 'CANCELLED') AS cancelled_total_monthly_fee_bucket,
        ROUND(AVG(first_known_monthly_fee), 2) AS avg_first_known_monthly_fee,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT_IF(status = 'ACTIVE') AS active_events,
        COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
    FROM with_windows
    GROUP BY DATE_TRUNC('YEAR', end_date_plus_months), CASE WHEN prev_monthly_fee >= 200 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    year_start,
    prev_monthly_fee_tier,
    cancelled_count,
    avg_first_known_monthly_fee,
    active_events,
    basic_count
FROM grouped;
