WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(monthly_fee * 0.85, 2) AS adjusted_monthly_fee,
        CASE WHEN plan IN ('enterprise', 'standard') THEN 'Group A' ELSE 'Other' END AS plan_group
    FROM subscriptions
    WHERE status IN ('EXPIRED', 'ACTIVE')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(monthly_fee, 2) OVER (PARTITION BY customer_id ORDER BY end_date, id) AS next_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY plan, customer_id) AS plan_sum_monthly_fee,
        SUM(adjusted_monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjusted_monthly_fee
    FROM filtered
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, d.credit_limit
    FROM with_windows e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(plan_sum_monthly_fee, 1) OVER (PARTITION BY plan ORDER BY plan_sum_monthly_fee, id) AS prev_plan_sum_monthly_fee,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM joined
),
filtered_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(credit_limit, 2) AS credit_limit_rounded,
        prev_plan_sum_monthly_fee / NULLIF(adjusted_monthly_fee, 0) AS prev_plan_sum_monthly_fee_per_adjusted_monthly_fee
    FROM enriched
    WHERE start_date > TO_DATE('2023-11-05')
      AND customer_id IS NOT NULL
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        status,
        CASE WHEN credit_limit_rounded >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_rounded_tier,
        COUNT(*) AS num_rows,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
    FROM filtered_subscriptions
    GROUP BY status, CASE WHEN credit_limit_rounded >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    status,
    credit_limit_rounded_tier,
    num_plans,
    basic_rows
FROM grouped
ORDER BY num_plans
