WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM subscriptions
    WHERE monthly_fee < 180
      AND end_date > TO_DATE('2023-09-25')
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.plan,
        e.start_date,
        c.segment,
        c.age
    FROM cleaned e
    INNER JOIN customers c
        ON e.customer_id = c.id
    WHERE c.segment = 'Consumer'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY age DESC NULLS LAST) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM ranked e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL '7' DAY
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee
    FROM attributed src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date >= sub.start_date
       AND src.start_date <= DATE_ADD(sub.end_date, 3)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    subscription_plan,
    CASE WHEN subscription_monthly_fee >= 228 THEN 'LARGE' ELSE 'SMALL' END AS subscription_monthly_fee_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    SORT_ARRAY(COLLECT_SET(subscription_plan_2)) AS subscription_plan_2_list,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    GROUPING(DATE_TRUNC('MONTH', start_date)) AS grouping_level
FROM with_window
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', start_date), subscription_plan, CASE WHEN subscription_monthly_fee >= 228 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('MONTH', start_date), subscription_plan), ())
