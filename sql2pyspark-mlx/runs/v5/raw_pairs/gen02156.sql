WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-07-16')
      AND start_date BETWEEN '2024-05-01' AND '2024-11-01'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM filtered s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.end_date >= sub.start_date
       AND s.end_date <= DATE_ADD(sub.end_date, 3)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        days_since_end_date,
        subscription_id,
        subscription_plan,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY start_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status, plan ORDER BY end_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_percent_rank
    FROM attributed
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan ORDER BY subscription_id DESC) AS dedup_rank
        FROM with_windows
    ) t
    WHERE dedup_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(start_date, 6) AS start_date_plus_months,
        ROUND(last_known_monthly_fee * 0.85, 2) AS last_known_monthly_fee_scaled
    FROM latest
    WHERE customer_id IS NOT NULL
      AND subscription_plan IS DISTINCT FROM 'enterprise'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        status,
        subscription_id,
        subscription_plan,
        last_known_monthly_fee,
        last_known_monthly_fee_2,
        monthly_fee_percent_rank,
        dedup_rank,
        start_date_plus_months,
        last_known_monthly_fee_scaled,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY days_since_end_date DESC, id, subscription_id) AS days_since_end_date_bucket,
        MIN(subscription_id) OVER (PARTITION BY status) AS status_min_subscription_id,
        AVG(monthly_fee_percent_rank) OVER (PARTITION BY customer_id ORDER BY last_known_monthly_fee_scaled, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_percent_rank_avg_6,
        ROUND(last_known_monthly_fee / SUM(last_known_monthly_fee) OVER (PARTITION BY status), 4) AS last_known_monthly_fee_share,
        NTILE(3) OVER (ORDER BY last_known_monthly_fee_2 DESC, id, subscription_id) AS last_known_monthly_fee_2_bucket
    FROM valid
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', start_date_plus_months) AS quarter_start,
    DATE_TRUNC('WEEK', start_date_plus_months) AS week_start,
    subscription_id,
    AVG(last_known_monthly_fee_share) FILTER (WHERE subscription_plan = 'enterprise') AS enterprise_avg_last_known_monthly_fee_share,
    COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_rows,
    COUNT(CASE WHEN subscription_plan = 'premium' THEN 1 END) AS premium_count,
    COUNT_IF(plan = 'standard') AS standard_events,
    ROUND(SUM(last_known_monthly_fee_scaled) / NULLIF(COUNT(CASE WHEN subscription_plan = 'standard' THEN 1 END), 0), 2) AS last_known_monthly_fee_scaled_per_standard,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM enriched
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', start_date_plus_months), DATE_TRUNC('WEEK', start_date_plus_months), subscription_id), (DATE_TRUNC('QUARTER', start_date_plus_months), DATE_TRUNC('WEEK', start_date_plus_months)))
